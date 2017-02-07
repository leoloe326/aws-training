#!/bin/bash

# Build an AMI using Packer

set -e

AMI_SPEC=${1:-"ami.json"}

echo "Build customized AMI..."

packer validate ./${AMI_SPEC}

packer build -color=false ${AMI_SPEC}
