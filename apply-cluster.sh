#!/bin/bash
#
# apply-cluster.sh applies the k8s cluster configuration to the currently
# configured cluster. This script may be safely run multiple times to load the
# most recent configurations.
#
# Example:
#
#   PROJECT=mlab-sandbox CLUSTER=scraper-cluster ./apply-cluster.sh

set -x
set -e
set -u

USAGE="PROJECT=<projectid> CLUSTER=<cluster> $0"
PROJECT=${PROJECT:?Please provide project id: $USAGE}
CLUSTER=${CLUSTER:?Please provide cluster name: $USAGE}
DATE_SKIP=${DATE_SKIP:-"0"}  # Number of dates to skip between each processed date (for sandbox).
TASK_FILE_SKIP=${TASK_FILE_SKIP:-"0"}  # Number of files to skip between each processed file (for sandbox).

# Apply templates
CFG=/tmp/${CLUSTER}-${PROJECT}.yml
kexpand expand --ignore-missing-keys k8s/${CLUSTER}/*/*.yml \
    --value GCLOUD_PROJECT=${PROJECT} \
    --value GIT_COMMIT=${TRAVIS_COMMIT} \
    --value DATE_SKIP=${DATE_SKIP} \
    --value TASK_FILE_SKIP=${TASK_FILE_SKIP} \
    > ${CFG}
cat ${CFG}

# This triggers deployment of the pod.
kubectl apply -f ${CFG}
