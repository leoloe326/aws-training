#!/bin/bash -v

SETUP_SCRIPT=dun-us-west-2/aws/setup.sh
SETUP_USER=ec2-user

aws s3 cp s3://${SETUP_SCRIPT} /home/${SETUP_USER}/setup.sh
chown ec2-user:ec2-user /home/${SETUP_USER}/setup.sh
chmod 700 /home/${SETUP_USER}/setup.sh
su - ec2-user -c "/home/${SETUP_USER}/setup.sh"
