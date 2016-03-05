# Makefile for yabf

QUIET := @
GO := $(if $(shell which gov),gov,go)

.PHONY: all test test-root test-generator


all:
	$(QUIET) cd main && $(GO) build

test: test-root test-generator

test-root:
	$(QUIET) $(GO) test -v

test-generator:
	$(QUIET) cd generator && $(GO) test -v

