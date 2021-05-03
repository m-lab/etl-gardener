#!/bin/bash
#
# Configure cluster, network, firewall and node-pools for gardener and etl.

set -x
set -e

USAGE="$0 <project> <region>"
PROJECT=${1:?Please provide the GCP project id, e.g. mlab-sandbox: $USAGE}
REGION=${2:?Please provide the cluster region, e.g. us-central1: $USAGE}

gcloud config unset compute/zone
gcloud config set project $PROJECT
gcloud config set compute/region $REGION
gcloud config set container/cluster data-processing

gcloud container node-pools delete parser-pool

gcloud container node-pools create parser-pool \
    --machine-type=n1-standard-16 \
    --enable-autoscaling --min-nodes=0 --max-nodes=2 \
    --enable-autorepair --enable-autoupgrade \
    --scopes storage-rw,compute-rw,datastore,cloud-platform \
    --node-labels=parser-node=true,storage-rw=true
