# Dynamic Protobuf Schema Generation from Unity Catalog

## Overview

Implemented dynamic protobuf descriptor generation from Databricks Unity Catalog table schema. This allows the databricks_zerobus sink to automatically fetch and generate the protobuf schema from the table definition instead of requiring a manually-created `.desc` file.

## What Was Implemented

### 1. New Schema Source Option

Added `UnityCatalog` variant to `SchemaSource` enum in `config.rs`:

```rust
pub enum SchemaSource {
    Path { ... },           // Existing: Load from .desc file
    UnityCatalog,           // NEW: Fetch from Unity Catalog API
}
```

### 2. Unity Catalog API Client

Created `unity_catalog_schema.rs` module with:

- **OAuth Authentication**: Gets access token using client credentials
- **Schema Fetching**: Calls Unity Catalog API to get table schema
- **Type Mapping**: Maps Databricks types to Protobuf types
- **Descriptor Generation**: Dynamically builds protobuf descriptor in memory

### 3. Type Mapping (Phase 1: Simple Types)

Currently supported Databricks → Protobuf type mappings:

| Databricks Type | Protobuf Type | Notes |
|----------------|---------------|-------|
| STRING         | String        | ✅ |
| INT            | Int32         | ✅ |
| BIGINT         | Int64         | ✅ |
| BOOLEAN/BOOL   | Bool          | ✅ |
| DOUBLE/FLOAT   | Double        | ✅ |
| TIMESTAMP      | String        | ISO 8601 format |
| BINARY         | Bytes         | ✅ |
| STRUCT         | String        | ⚠️ Serialized as string (temp) |
| ARRAY          | String        | ⚠️ Serialized as string (temp) |

**Complex types (STRUCT, ARRAY) are temporarily treated as strings with a warning.** For full complex type support, use an explicit schema file.

### 4. Flow

```
1. User Config:
   schema = { type = "unity_catalog" }

2. Service Initialization:
   - Descriptor set to None (will fetch lazily)

3. First Event Arrives:
   - Calls get_descriptor_or_infer()
   - Fetches OAuth token
   - Calls UC API: GET /api/2.0/unity-catalog/tables/{table_name}
   - Parses column schema
   - Maps types to protobuf
   - Generates MessageDescriptor in memory
   - Caches for subsequent events

4. Stream Creation:
   - Uses generated descriptor in TableProperties
   - Sends to Databricks Zerobus

5. Event Encoding:
   - Encodes events using the descriptor
   - Ingests to Unity Catalog table
```

## Configuration Examples

### Dynamic Schema (NEW)
```json
{
    "sinks": {
        "zerobus": {
            "type": "databricks_zerobus",
            "table_name": "main.schema.table",
            "schema": {
                "type": "unity_catalog"
            },
            ...
        }
    }
}
```

### Explicit Schema (Existing)
```json
{
    "schema": {
        "type": "path",
        "path": "/path/to/schema.desc",
        "message_type": "MyMessage"
    }
}
```

### No Schema (Existing - Infer from Events)
```json
{
    "schema": null  // or omit the field
}
```

## Risk Assessment

### 1. Complex Type Mapping: ✅ **Mitigated**
- **Risk**: Incorrect mapping of complex nested types
- **Mitigation**:
  - Phase 1 focuses on simple types
  - Complex types (STRUCT/ARRAY) serialize as string with warning
  - Users can still use explicit schema files for complex tables
  - Future: Parse `type_json` field for full nested type support

### 2. Type Precision: ✅ **Low Risk**
- TIMESTAMP → String (ISO 8601) - standard approach
- All simple types have direct mappings
- No precision loss for basic types

### 3. Schema Evolution: ✅ **Handled**
- Schema fetched on first use
- Cached for the session
- If table schema changes mid-run, requires restart
- Future: Add optional schema version checking/refresh

### 4. Partition Columns: ✅ **Handled Correctly**
- Partition columns are regular columns with `partition_index` metadata
- Included in the protobuf descriptor like any other column
- Databricks handles partitioning automatically based on values

## Files Changed

1. `src/sinks/databricks_zerobus/config.rs` - Added `UnityCatalog` variant
2. `src/sinks/databricks_zerobus/unity_catalog_schema.rs` - NEW file with API client and type mapping
3. `src/sinks/databricks_zerobus/service.rs` - Updated to fetch schema dynamically
4. `src/sinks/databricks_zerobus/mod.rs` - Added new module
5. `demo_zerobus_dynamic_schema.json` - Example configuration

## Testing

To test with your demo configuration:

```bash
# Build Vector with the feature
cargo build --features sinks-databricks-zerobus

# Run with dynamic schema
./target/debug/vector --config demo_zerobus_dynamic_schema.json
```

Expected behavior:
1. On startup, fetches table schema from Unity Catalog API
2. Generates protobuf descriptor for `service_health_event` table
3. Warns about complex types (like `service_extra`, `_log_metadata`) being serialized as strings
4. Ingests demo logs to the table

## Future Enhancements

### Phase 2: Complex Type Support
- Parse `type_json` field for nested STRUCT definitions
- Recursively generate nested message types
- Handle ARRAY types with proper repeated fields
- Support MAP types

### Phase 3: Schema Caching
- Cache fetched schemas to disk
- Add version/checksum validation
- Support schema refresh without restart

### Phase 4: Schema Evolution
- Detect schema changes
- Handle backward-compatible updates
- Alert on breaking changes

## Limitations

**Current limitations (Phase 1):**
- Complex STRUCT columns are serialized as JSON strings
- ARRAY columns are serialized as JSON strings
- No schema caching between Vector restarts
- No automatic schema refresh on table changes

**Recommended approach for complex tables:**
Use an explicit schema file (existing `Path` option) for tables with:
- Deeply nested STRUCTs (like your `service_extra` column)
- Complex ARRAY types
- Strict type requirements

## Benefits

✅ No need to run `protoc` and generate `.desc` files manually
✅ Schema automatically matches the table definition
✅ Simpler configuration (just specify `type: unity_catalog`)
✅ Reduced maintenance overhead
✅ Works well for tables with simple types
✅ Fallback to explicit schema for complex types still available
