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


## k8s cluster and network

Gardener will soon provide a job allocation service to the ETL parsers.  To do this, we run gardener in a cluster that uses a custom internal network, and reserve a static ip address at 10.100.1.2 for the Gardener service.

The network is set up like this (after setting up the kubectl config appropriately):

```
gcloud compute networks subnets create default-gardener --network=default --range=10.100.0.0/16
```

```
gcloud --project=mlab-sandbox compute addresses create default-gardener-etl-gardener --region=us-east1 --subnet=default-gardener --addresses=10.100.1.2
```

```
gcloud container clusters create data-processing --region=us-east1 --num-nodes 2 --scopes=bigquery,taskqueue,compute-rw,storage-ro,service-control,service-management,datastore   --node-labels=gardener-node=true   --enable-autorepair   --enable-autoupgrade   --image-type=cos   --machine-type=n1-standard-4 --labels=data-processing=true --subnetwork=default-gardener
```

### Accessing from ETL parser instances

ETL Parsers will access the Gardener service through the custom subnetwork.  This requires adding to the network section of the App Engine Flex config:
```
network:
  subnetwork_name: default-gardener
```

NOTE: In addition to adding the subnetwork_name to the config, the app engine instances must run in the same region as the data-processing cluster, i.e. us-east1.

## Node Pools (deprecated)

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
