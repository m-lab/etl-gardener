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

# Remove boto config; recommended for datastore emulator.
rm -f /etc/boto.cfg || :

# Start datastore emulator.
gcloud beta emulators datastore start --no-store-on-disk &

t1=$( date +%s )
until nc -z localhost 8081 ; do
  sleep 1
  t2=$( date +%s )
  # Allow up to a minute for emulator to start up.
  if [[ $t2 -gt $(( $t1 + 60 )) ]] ; then
    echo "ERROR: failed to start or detect datastore emulator"
    break
  fi
done
$(gcloud beta emulators datastore env-init)
go test -v -tags=integration -coverprofile=_integration.cov ./...

# Shutdown datastore emulator.
kill %1
