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
	@echo "  bill         show AWS billing"
	@echo "  benchmark    benchmark AWS resources"
	@echo "  clean        clean project"

.PHONY: prepare
prepare:
	@cd scripts/ && PREFIX=$(PREFIX) ./install_packages.sh

.PHONY: configure
configure:
	@cd scripts/ && ./configure.sh

.PHONY: bill
bill:
	@cd scripts/ && ./bill.py

.PHONY: benchmark
benchmark:
	@echo "Bechmarking AWS resources..."
	@cd scripts/ && ./ec2_benchmark.py --wait --verbose 2

.PHONY: debug
debug:
	@echo "Debugging entire website..."
	BOKEH_LOG_LEVEL=debug bokeh serve taxi

.PHONY: clean
clean:
	@cd scripts/ && ./ec2_benchmark.py --clean --verbose 2
	@echo "Clean the project..."
	rm -rf *.pyc */*.pyc
