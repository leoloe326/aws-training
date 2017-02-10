#
# Makefile for Infrastructure Management
#
# Nan Dun <nan.dun@acm.org>
#

# Configure variables
TF_CONFIG_VARS := config.tfvars

######## DO NOT EDIT BELOW THIS UNLESS YOU KNOW WHAT YOU ARE DOING ########
TF := terraform
TF_FLAGS := -var-file=$(TF_CONFIG_VARS)
USER ?= `whoami`
AS  := ansible
ASP := ansible-playbook

all: help
help:
	@echo "usage: make <action>"
	@echo "Available actions are:"
	@echo "  ami         build customized AMI"
	@echo "  plan        create infrastructure plan"
	@echo "  show        show infrasctructure plan"
	@echo "  apply       apply infrastructure plan"
	@echo "  inventory   create Ansible inventory file"
	@echo "  deploy      deploy ansible playbook"
	@echo "  destroy     destroy infrastructure"

.PHONY: ami
ami:
	./build_ami.sh

.PHONY: plan
plan:
	@echo "Generating infrastructure plan..."
	$(TF) validate
	$(TF) plan $(TF_FLAGS)

.PHONY: show
show:
	@echo "Showing infrastructure plan..."
	$(TF) show

.PHONY: apply
apply:
	@echo "Applying infrastructure plan..."
	$(TF) apply $(TF_FLAGS)

.PHONY: inventory
inventory:
	@echo "Creating Ansible inventory..."
	$(TF) output -json | ./json2ini.py --keywords webserver,mapper,reducer -o hosts

.PHONY: deploy
deploy:
	@echo "Deploying Ansible playbook..."
	$(ASP) cluster.yaml

.PHONY: destroy
destroy:
	@echo "Destroying infrastructure..."
	$(TF) destroy $(TF_FLAGS)
