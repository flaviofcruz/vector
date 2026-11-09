Fix the `file_source` read order when an archive finishes: finished archives were removed with `swap_remove`, which moved the hot active file to the front of the oldest-first read order and starved older rotated segments until kubelet deleted them unread; they now keep their order (`shift_remove`). Adds a regression test (ES-2138642).
https://github.com/databricks-eng/vector/pull/906
