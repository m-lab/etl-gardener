Gardener provides both a service for reprocessing and deduplicating, and
will (soon) provide a command line tool for doing manual reprocessing and
deduplicating operations.

The command line should be run from an environment logged into gcloud in
the appropriate project.

For example:
```bash
go run cmd/gardener/gardener.go -project mlab-oti -day 2017/10/01
```

The batch pipeline job also runs in and writes to mlab-oti.
