#!/bin/bash

set -e

BUCKET=dun-us-west-2/aws
KEY=id_bitbucket_deploy
REPO=git@bitbucket.org:bittiger-aws/aws.git

echo "Executing as `whoami`"

echo "Installing packages..."
sudo yum install -y git geos-devel
sudo /usr/local/bin/pip install -U pandas shapely

echo "Cloning repository..."
aws s3 cp s3://${BUCKET}/${KEY} ~/.ssh/id_bitbucket_deploy
chmod 400 ~/.ssh/id_bitbucket_deploy
eval `ssh-agent`
ssh-add ~/.ssh/id_bitbucket_deploy
echo -e "Host bitbucket.org\n  StrictHostKeyChecking no\n" >> ~/.ssh/config
chmod 600 ~/.ssh/config
git clone ${REPO} ~/aws
