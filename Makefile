# Makefile for yabf

QUIET    := @
MKDIR    := mkdir -p
RM       := rm -rf
GO       := $(if $(shell which gov),gov,go)

root_dir := $(CURDIR)
cloudtable_idl := $(root_dir)/binding/cloudtable.thrift
gen_dir  := $(root_dir)/binding/cloudtable-gen

.PHONY: all gen test test-root test-generator clean

all:
	$(QUIET) cd main && $(GO) build

all: gen

gen: $(gen_dir)

$(gen_dir): $(cloudtable_idl)
	$(QUIET) $(MKDIR) $(gen_dir) && thrift --gen go -out $(gen_dir) $<

test: test-root test-generator

test-root:
	$(QUIET) $(GO) test -v

test-generator:
	$(QUIET) cd generator && $(GO) test -v

clean:
	$(QUIET) $(RM) $(gen_dir)

