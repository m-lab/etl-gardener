# gardener

| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-gardener.svg?branch=master)](https://travis-ci.org/m-lab/etl-gardener) | [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/etl-gardener)](https://goreportcard.com/report/github.com/m-lab/etl-gardener) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-gardener/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-gardener?branch=master) |

Gardener provides services for maintaining and reprocessing M-Lab data.

## Overview

The v2 data pipeline depends on the gardener for daily and historical
processing.

Daily processing is daily around 10:30 UTC to allow time for nodes to upload
data and the daily transfer jobs to copy data to the public archives.
Historical processing is currently continuous. As soon as one pass has
completed, the gardener starts again from its start date.

For both of these modes, gardener issues Jobs (dates) to parsers that request
them. The parsers will enuemerate all files for that date and parse each, and
report status updates to the gardener for the Job date until all are complete.

### Jobs API

Parsers request new date jobs from the gardener via the Jobs API. The API
supports four operations:

* `/job` - return the next job
* `/update` - update the status of a job
* `/error` - report a job error
* `/heartbeat` - update progress for a job and tell gardener that work is
  still in progress.

These resources are available on the `-gardener_addr`.

### Status Page

Gardener maintains a status page on a separate status port, that summarizes
recent jobs, current state, and any errors. Jobs transition through the
following stages:

* `Parsing` - gardener has issued a job and a parser is working on it.
* `postProcessing` - the parser completed parsing a job.
* `Loading` - gardener loads the parser output from GCS to a temporary BigQuery table.
* `Deduplicating` - gardener deletes duplicate rows from the temporary table.
* `Copying` - gardener copies rows from the temporary table to the "raw" table.
* `Deleting` - gardener deletes the job rows from the temporary table.
* `Joining` - after gardener processes all raw tables for a date, it may combine
  the raw table with other raw tables in a materialized join.
* `Complete` - all steps were completed successfully.

The status page is available on the `-status_port`.

## Local Development with Parser

Both the gardener and parsers support a local development mode. To run both
follow the following steps.

Create a test configuration, e.g. `test.yml`, with a subset of the production
configuration that includes only the datatype you are working with.

Run the gardener v2 ("manager" mode) with local writer support:

```sh
go get ./cmd/gardener
~/bin/gardener \
    -project=mlab-sandbox \
    -status_port=:8082 \
    -gardener_addr=localhost:8081 \
    -prometheusx.listen-address=:9991 \
    -config_path=config/test.yml \
    -saver.backend=local \
    -saver.dir=singleton
```

Run the parser to target the local gardener:

```sh
go get ./cmd/etl_worker
gcloud auth application-default login
~/bin/etl_worker \
  -gardener_addr=localhost:8081 \
  -output_dir=./output \
  -output=local
```

If the `start_date` in the input `test.yml` for your datatype includes archive
files, then the parser should begin parsing archives immediately and writing them to
the `./output` directory.

## Unit Testing

Some of the gardener packages depend on complex, third-party services. To
accommodate these the gardener unit tests are split into three categories:

* Run standard unit tests

  ```sh
  go test -v ./...
  ```

* Run integration unit tests

  Integration unit tests depend on state in the mlab-testing GCP project, and
  require credentials to access this project. Members of M-Lab staff should be
  able to use their existing cloud credentials and application default
  credentials.

  ```sh
  go test -v -tags=integration -coverprofile=_integration.cov ./...
  ```

* Run race unit tests

  ```sh
  go test -race -v ./...
  ```
