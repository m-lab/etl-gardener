# gardener
| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-gardener.svg?branch=master)](https://travis-ci.org/m-lab/etl-gardener) | | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-gardener/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-gardener?branch=master) |

[![Waffle.io](https://badge.waffle.io/m-lab/etl-gardener.svg?title=Ready)](http://waffle.io/m-lab/etl-gardener)



## Gardener provides services for maintaining and reprocessing mlab data.

## Unit Testing
Unit tests now use datastore with a fake project (xyz).  This is enabled by
running the datastore emulator, which should be started like this:

gcloud beta emulators datastore start --no-store-on-disk &

We may want to arrange for the unit tests themselves to launch the emulator.
