#!/bin/bash

# Install Required Packages on Your Unix/Linux System

PREFIX=${PREFIX:-${HOME}/local}

PYTHON_PACKAGES=${PYTHON_PACKAGES:-"awscli aws-shell boto boto3 bokeh paramiko \
shapely bytebuffer jmespath-terminal ansible flexx"}

PACKER_VERSION=${PACKER_VERSION:-0.12.2}
TERRAFORM_VERSION=${TERRAFORM_VERSION:-0.8.5}

# Install Python Packages
echo "Installing ${PYTHON_PACKAGES}..."
sudo pip install -U ${PYTHON_PACKAGES}

# Install Packer
PACKER_URL="https://releases.hashicorp.com/packer/${PACKER_VERSION}"
PACKER_BIN=$(command -v packer)

if [ ! -z "${PACKER_BIN}" ]; then
    echo "Packer $(packer -v) already installed at ${PACKER_BIN}, skip..."
else
	echo "Installing Packer..."
    curl ${PACKER_URL}/packer_${PACKER_VERSION}_darwin_amd64.zip -o packer.zip
	mkdir -p ${PREFIX}/bin
    unzip packer.zip -d ${PREFIX}/bin
    rm -rf packer.zip
fi

# Install Terraform
TERRAFORM_URL="https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}"
TERRAFORM_BIN=$(command -v terraform)
if [ ! -z "${TERRAFORM_BIN}" ]; then
    echo "$(terraform -v) already installed at ${TERRAFORM_BIN}, skip..."
else
	echo "Installing Terraform..."
    curl ${TERRAFORM_URL}/terraform_${TERRAFORM_VERSION}_darwin_amd64.zip -o terraform.zip
	mkdir -p ${PREFIX}/bin
    unzip terraform.zip -d ${PREFIX}/bin
    rm -rf terraform.zip
fi

# Install DynamoDB Local
# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
DDB_URL="https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"
DDB_PATH=$PREFIX/dynamodb
if [ ! -d "$DDB_PATH" ]; then
	mkdir -p $DDB_PATH
	curl $DDB_URL | tar zxv -C $DDB_PATH
fi
