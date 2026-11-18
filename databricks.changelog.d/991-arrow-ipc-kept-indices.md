Fix the `arrow` build of `v0.53-custom` after #961 and #977 merged: the `arrow_ipc` batch serializer now returns its kept event indices (all of them) like the other serializers.
https://github.com/databricks-eng/vector/pull/991
