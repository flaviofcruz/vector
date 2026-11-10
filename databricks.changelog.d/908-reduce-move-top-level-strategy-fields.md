Make the `reduce` transform move a merge-strategy field out of the event instead of deep-cloning it when the strategy is on a plain top-level field and no other strategy path nests below it; merged output is unchanged. Cuts per-event CPU in the logging-agent multiline stage (ES-2138642).
https://github.com/databricks-eng/vector/pull/908
