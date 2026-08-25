Fixed the `wire_to_arrow` encoder to bind proto `map<K, V>` entries by ordinal position (key first, value second) rather than by Arrow field name. Delta/Spark-derived schemas surface map columns as `Map<Struct(keys, values)>`, whose entry field names don't match proto `MapEntry`'s `key`/`value`; the encoder previously rejected these at serializer build, dropping every batch for map-bearing tables. Entry structs that declare more than two fields are still rejected.

authors: gzc
