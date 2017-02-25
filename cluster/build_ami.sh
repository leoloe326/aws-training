#!/bin/bash

# Build an AMI using Packer

set -e

AMI_SPEC=${1:-"amzn-ami.json ecs-ami.json"}

for ami in ${AMI_SPEC}; do
	echo "Build customized AMI from ${ami}..."
	packer validate ./${ami}
	packer build -color=false ${ami}
done
