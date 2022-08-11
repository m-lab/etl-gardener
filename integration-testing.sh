#!/bin/bash

# Exit on error.
set -e

# Install test credentials for authentication:
# * gcloud commands will use the activated service account.
# * Go libraries will use the GOOGLE_APPLICATION_CREDENTIALS.
if [[ -z "$SERVICE_ACCOUNT_mlab_testing" ]] ; then
  echo "ERROR: testing service account is unavailable."
  exit 1
fi

echo "$SERVICE_ACCOUNT_mlab_testing" > $PWD/creds.json
# Make credentials available for Go libraries.
export GOOGLE_APPLICATION_CREDENTIALS=$PWD/creds.json
if [ -f "/builder/google-cloud-sdk/path.bash.inc" ]; then
  # Reset home directory for container.
  HOME=/builder
fi
# Make credentials available for gcloud commands.
travis/activate_service_account.sh SERVICE_ACCOUNT_mlab_testing

# NOTE: do this after setting the service account.
gcloud config set project mlab-testing

# Rsync the mlab-testing ndt directory to match expected content.
pushd testfiles
./sync.sh
popd

go test -v -tags=integration -coverprofile=_integration.cov ./...
