# gardener
| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-gardener.svg?branch=master)](https://travis-ci.org/m-lab/etl-gardener) | [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/etl-gardener)](https://goreportcard.com/report/github.com/m-lab/etl-gardener) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-gardener/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-gardener?branch=master) |

[![Waffle.io](https://badge.waffle.io/m-lab/etl-gardener.svg?title=Ready)](http://waffle.io/m-lab/etl-gardener)



## Gardener provides services for maintaining and reprocessing mlab data.

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
