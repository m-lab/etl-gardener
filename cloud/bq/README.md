sanity.go provides functions for sanity checking whether a reprocessed
partition is similar to the original partition.

SanityCheckAndCopy checks that various metrics are sensible, then copies
the table into the corresponding partition in __destination_table__.

## Useful bits:

1. bq show --format=prettyjson measurement-lab.batch.ndt_* will give
 the summary for the whole set.  Using HTTP, this would be a get request:
 ```
GET https://www.googleapis.com/bigquery/v2/projects/projectId/datasets/datasetId/tables/tableId

 ```

## Other considerations:
1. Should run the task count check?  Unfortunately, there may be task
files that don't result in any rows.  How to deal with that?

