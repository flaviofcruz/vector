# Unity Catalog Field Numbering Evidence

## Claim
Unity Catalog API provides `position` for top-level columns only. Nested fields have NO position/ordering information suitable for protobuf field numbering.

## Proof from Actual API Response

### Top-Level Column (service_extra)

```json
{
  "name": "service_extra",
  "position": 13,              // ✅ Top-level HAS position
  "type_name": "STRUCT",
  "nullable": true,
  "type_json": "{...}"         // Contains nested structure
}
```

### Nested Fields (Inside type_json)

```json
{
  "name": "service_extra",
  "type": {
    "type": "struct",
    "fields": [
      {
        "name": "jobs",           // ❌ NO position field
        "type": {
          "type": "struct",
          "fields": [
            {
              "name": "job_id",   // ❌ NO position field
              "type": "long",
              "nullable": true,
              "metadata": {
                "delta.columnMapping.id": 68,      // ← Delta internal ID
                "delta.columnMapping.physicalName": "job_id"
              }
            },
            {
              "name": "task_run_id", // ❌ NO position field
              "type": "long",
              "nullable": true,
              "metadata": {
                "delta.columnMapping.id": 69,      // ← Delta internal ID
                "delta.columnMapping.physicalName": "task_run_id"
              }
            }
          ]
        }
      }
    ]
  }
}
```

## What Unity Catalog Provides

### Top-Level Columns
✅ **position** - Can be used directly as protobuf field number

```rust
for column in table_schema.columns {
    let field_number = column.position;  // ✅ Stable, unique, ordered
}
```

### Nested Fields (Inside type_json)
❌ **NO position field**
✅ **delta.columnMapping.id** - Delta Lake internal ID
✅ **name** - Field name
❌ **NO ordering guarantee**

```rust
for field in struct_fields {
    let field_number = ???  // ❌ No position!

    // What we have:
    // - field.metadata.delta.columnMapping.id: 68
    //   → This is Delta's internal ID, not suitable for protobuf
    //   → Could change if table is recreated
    //   → Not consecutive (68, 69, 122, 132, 159, ...)

    // - Iteration order in JSON
    //   → NOT guaranteed stable
    //   → Could change between API calls
    //   → Databricks might reorder fields
}
```

## Why delta.columnMapping.id Doesn't Work

From the actual data:
```
job_id               → delta.columnMapping.id: 68
task_run_id          → delta.columnMapping.id: 69
job_run_id           → delta.columnMapping.id: 70
run_termination_reason → delta.columnMapping.id: 71
is_serverless        → delta.columnMapping.id: 122  // ← Gap!
cluster_termination_reason → delta.columnMapping.id: 132  // ← Another gap!
customer_impacting_startup_delay → delta.columnMapping.id: 159  // ← Huge gap!
```

**Problems with using these IDs:**
1. ❌ **Not consecutive** - Protobuf field numbers should be 1, 2, 3, 4...
2. ❌ **Large gaps** - Wastes encoding space (varint inefficiency)
3. ❌ **Delta-specific** - Tied to Delta Lake internals
4. ❌ **Could change** - If table is recreated, IDs might differ
5. ❌ **Goes up to 500+** - Some fields have ID 500+, protobuf best practice is < 15 for frequently used fields

## Protobuf Field Number Requirements

From Protobuf documentation:
- Field numbers 1-15 take 1 byte to encode (most efficient)
- Field numbers 16-2047 take 2 bytes
- Field numbers must be unique within a message
- Field numbers should be stable (never change after deployment)
- Field numbers should be consecutive for optimal encoding

**Example:**
```protobuf
message Jobs {
  int64 job_id = 1;              // ✅ Efficient (1 byte tag)
  int64 task_run_id = 2;         // ✅ Efficient (1 byte tag)
  int64 job_run_id = 3;          // ✅ Efficient (1 byte tag)
}

// VS using Delta IDs:

message Jobs {
  int64 job_id = 68;             // ❌ Inefficient (2 byte tag)
  int64 task_run_id = 69;        // ❌ Inefficient (2 byte tag)
  int64 job_run_id = 70;         // ❌ Inefficient (2 byte tag)
  string run_termination_reason = 71;
  bool is_serverless = 122;      // ❌ Very inefficient, gaps waste space
  string cluster_termination_reason = 132;
  bool customer_impacting_startup_delay = 159;
}
```

## Possible Solutions (All Have Tradeoffs)

### Option 1: Sequential Numbering
```rust
let mut field_num = 1;
for field in struct_fields {
    assign_field_number(field_num);
    field_num += 1;
}
```
✅ Efficient encoding (1-15 range)
❌ Breaks if Databricks reorders fields in type_json
❌ Order in JSON not guaranteed stable

### Option 2: Use Delta IDs Directly
```rust
for field in struct_fields {
    let field_num = field.metadata.delta_column_mapping_id;
}
```
✅ Stable (tied to Delta's internal mapping)
❌ Inefficient encoding (large numbers, gaps)
❌ Delta-specific, might change on table recreate
❌ Not consecutive

### Option 3: Hash Field Path
```rust
fn field_number(path: &str) -> u32 {
    let hash = hash("service_extra.jobs.job_id");
    (hash % 18999) + 1  // Keep in valid range 1-19000
}
```
✅ Deterministic
✅ Stable across API calls
❌ Collision risk (birthday paradox)
❌ Hard to debug
❌ Random-looking numbers

### Option 4: Stable Hash with Collision Detection
```rust
let mut assigned = HashMap::new();
for field in all_fields {
    let hash = stable_hash(field.path);
    let mut num = hash % 18999 + 1;

    // Handle collisions
    while assigned.contains_key(num) {
        num = (num + 1) % 18999;
    }
    assigned.insert(num, field);
}
```
✅ Deterministic
✅ Handles collisions
❌ Complex
❌ Numbers not consecutive
❌ Still risk of running out of space with many collisions

### Option 5: Store External Mapping
```toml
# In Vector config
[sinks.zerobus.field_mapping]
"service_extra.jobs.job_id" = 1
"service_extra.jobs.task_run_id" = 2
...
```
✅ Fully stable
✅ User controls numbering
✅ Can optimize for encoding efficiency
❌ Extra configuration burden
❌ Manual maintenance
❌ Error-prone

## Databricks Unity Catalog API Documentation

**Relevant fields returned for columns:**
- `name` - Column name
- `position` - Position in table (top-level only)
- `type_name` - Type name (e.g., "STRUCT", "ARRAY")
- `type_text` - Human-readable type
- `type_json` - Full nested type definition (JSON string)
- `nullable` - Whether null values allowed
- `partition_index` - If partition column (optional)

**Documentation:** https://docs.databricks.com/api/workspace/tables/get
**Note:** Documentation does not mention any ordering/position for nested fields in `type_json`

## Conclusion

**The claim is TRUE:**
1. ✅ Top-level columns have `position` field
2. ❌ Nested fields (in `type_json`) have NO position field
3. ❌ `delta.columnMapping.id` exists but unsuitable for protobuf numbering
4. ❌ No ordering guarantee in `type_json` structure
5. ❌ Must generate field numbers ourselves with no perfect solution

**This is the core challenge** for implementing full complex type support. Every solution has significant tradeoffs.
