#!/bin/bash
#
# Configure cluster, network, firewall and node-pools for gardener and etl.

set -x
set -e

USAGE="$0 <project> <region>"
PROJECT=${1:?Please provide the GCP project id, e.g. mlab-sandbox: $USAGE}
REGION=${2:?Please provide the cluster region, e.g. us-central1: $USAGE}

gcloud config set project $PROJECT
gcloud config set compute/region $REGION
gcloud config set container/cluster data-processing

# The network for comms among the components has to be created first.
if gcloud compute networks list | grep "^data-processing "; then
  # TODO - networks can be updated!!
  echo "Network already exists"
else
  gcloud compute networks create data-processing --subnet-mode=custom \
    --description="Network for communication among backend processing services."
fi

# This allows internal connections between components.
if gcloud compute firewall-rules list | grep "^dp-allow-internal "; then
  # TODO - firewall rules can be updated!!
  echo "Firewall rule dp-allow-internal already exists"
else
  gcloud compute firewall-rules create \
    dp-allow-internal --network=data-processing \
    --allow=tcp:0-65535,udp:0-65535,icmp --direction=INGRESS \
    --source-ranges=10.128.0.0/9,10.100.0.0/16 \
    --description='Allow internal traffic from anywhere'
fi

# Then add the subnet
# Subnet has the same name and address range across projects, but each is in a distinct (data-processing) VPC network."
if gcloud compute networks subnets list --network=data-processing | grep "^dp-gardener "; then
  # TODO - subnets can be updated!!
  echo "subnet data-processing/dp-gardener already exists"
else
  gcloud compute networks subnets create dp-gardener \
    --network=data-processing --range=10.100.0.0/16 \
    --enable-private-ip-google-access \
    --description="Subnet for gardener,etl,annotation-service."
fi

# And define the static IP address that will be used by etl parsers to reach gardener.
if gcloud compute addresses list --filter=region=\"region:($REGION)\" | grep "^etl-gardener "; then
  echo "subnet data-processing/dp-gardener already exists"
else
  gcloud compute addresses create etl-gardener \
    --region=$REGION
    --subnet=dp-gardener --addresses=10.100.1.2
fi

# Now we can create the cluster.
# It includes a default node-pool, though it isn't actually needed?
gcloud container clusters create data-processing \
  --network=data-processing --subnetwork=dp-gardener \
  --enable-autorepair --enable-autoupgrade \
  --scopes=bigquery,taskqueue,compute-rw,storage-ro,service-control,service-management,datastore \
  --num-nodes 2 --image-type=cos --machine-type=n1-standard-4 \
  --node-labels=gardener-node=true --labels=data-processing=true


# Define or update the role for etl-parsers
gcloud --project=$PROJECT iam roles update etl_parser --file=etl_parser_role.json
# Update service-account with the appropriate ACLS.
gcloud projects add-iam-policy-binding mlab-sandbox \
  --member=serviceAccount:etl-k8s-parser@mlab-sandbox.iam.gserviceaccount.com \
  --role=projects/mlab-sandbox/roles/parser_k8s

# Set up node pools for parser and gardener pods.
# Parser needs write access to storage.  Gardener needs only read access.
# TODO - narrow the cloud-platform scope? https://github.com/m-lab/etl-gardener/issues/308
if gcloud container node-pools describe parser-pool; then
  gcloud container node-pools update parser-pool \
    --num-nodes=1 --machine-type=n1-standard-8 \
    --enable-autorepair --enable-autoupgrade \
    --scopes storage-rw,compute-rw,datastore,cloud-platform \
    --node-labels=parser-node=true \
    --service-account=etl-k8s-parser@{$PROJECT}.iam.gserviceaccount.com
else
  gcloud container node-pools create parser-pool \
    --num-nodes=1 --machine-type=n1-standard-8 \
    --enable-autorepair --enable-autoupgrade \
    --scopes storage-rw,compute-rw,datastore,cloud-platform \
    --node-labels=parser-node=true \
    --service-account=etl-k8s-parser@{$PROJECT}.iam.gserviceaccount.com
fi

# TODO - narrow the cloud-platform scope? https://github.com/m-lab/etl-gardener/issues/308
gcloud container node-pools create gardener-pool \
  --num-nodes=1 --machine-type=n1-standard-2 \
  --enable-autorepair --enable-autoupgrade \
  --scopes storage-ro,compute-rw,datastore,cloud-platform \
  --node-labels=gardener-node=true \
  --service-account=etl-k8s-parser@{$PROJECT}.iam.gserviceaccount.com

# Setup node-pool for prometheus
gcloud container node-pools create prometheus-pool \
  --num-nodes=1 --machine-type=n1-standard-4 \
  --enable-autorepair --enable-autoupgrade \
  --node-labels=prometheus-node=true

