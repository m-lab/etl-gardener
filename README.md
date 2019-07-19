# gardener
| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-gardener.svg?branch=master)](https://travis-ci.org/m-lab/etl-gardener) | [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/etl-gardener)](https://goreportcard.com/report/github.com/m-lab/etl-gardener) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-gardener/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-gardener?branch=master) |

[![Waffle.io](https://badge.waffle.io/m-lab/etl-gardener.svg?title=Ready)](http://waffle.io/m-lab/etl-gardener)

Gardener provides services for maintaining and reprocessing M-Lab data.

## Unit Testing

Travis now uses the datastore emulator to better support unit tests.
Tests continue to use mlab-testing, so that they can be run manually from
workstation without configuring the emulator.

To start the emulator, .travis.yml now includes:

```base
- gcloud components install beta
- gcloud components install cloud-datastore-emulator
- gcloud beta emulators datastore start --no-store-on-disk &
- sleep 2 # allow time for emulator to start up.
- $(gcloud beta emulators datastore env-init)
```

You probably don't want to do this on your local machine, as it will leave
your local machine configured to use datastore emulation.  So be aware
that if you do, you'll want to clean up the `DATASTORE_` environment variables.


## Node Pools

Gardener runs in the GKE data-processing-cluster. 

Each cluster includes a node-pool reserved for Gardener deployments, created
using the following command line:

```
gcloud --project=mlab-sandbox container node-pools create gardener-pool \
  --cluster=data-processing-cluster \
  --num-nodes=3 \
  --scopes=bigquery,taskqueue,compute-rw,storage-ro,service-control,service-management,datastore \
  --node-labels=gardener-node=true \
  --enable-autorepair \
  --enable-autoupgrade \
  --image-type=cos \
  --machine-type=n1-standard-8
```
