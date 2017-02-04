#
# Makefile for BitTiger AWS Data Processing Infrastructure Lecture
# Copyright 2017 Nan Dun <nan.dun@acm.org>
#

# Configure variables
PREFIX := $(HOME)/local

# Don't modify unless you know what you are doing
PATH := $(PREFIX)/bin:$(PATH)

all: help
help:
	@echo "usage: make <action>"
	@echo "Available actions are:"
	@echo "  prepare      install prerequisite software packages"
	@echo "  configure    configure AWS environment"
	@echo "  benchmark    benchmark AWS resources"
	@echo "  clean        clean project"

.PHONY: prepare
prepare:
	@cd scripts/ && PREFIX=$(PREFIX) ./install_packages.sh

.PHONY: configure
configure:
	@cd scripts/ && ./configure.sh

.PHONY: benchmark
benchmark:
	@echo "Bechmarking AWS resources..."
	@cd scripts/ && ./ec2_benchmark.py --wait --verbose 2

.PHONY: clean
clean:
	@cd scripts/ && ./benchmark.py --clean --verbose 2
	@echo "Clean the project..."
	rm -rf *.pyc
