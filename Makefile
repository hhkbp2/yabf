# Makefile for yabf

QUIET    := @
MKDIR    := mkdir -p
RM       := rm -rf
MV       := mv
SED      := sed
GIT      := git
GO       := $(if $(shell which gov),gov,go)

root_dir := $(CURDIR)
cloudtable_idl := $(root_dir)/binding/cloudtable.thrift
gen_dir  := $(root_dir)/binding/cloudtable-gen
bin_dir  := ./main
bin_targets := $(patsubst %.go,%,$(wildcard $(bin_dir)/*.go))
version_file := version.go
version_source_file := $(root_dir)/$(version_file)
source_files := $(filter-out %$(version_file),$(filter-out %test.go,$(shell find $(root_dir) -path $(gen_dir) -prune -o -name '*.go')))


.PHONY: all gen test test-root test-generator clean

all: $(bin_targets)

$(bin_targets): $(gen_dir) $(source_files)

# write git latest version to version file
# $(call update-version)
define update-version
  $(QUIET) $(SED) -e "s/\\(.*\"\\)[^\"]*\\(\".*\\)/\\1$$($(GIT) rev-parse --short HEAD)\\2/g" < $(version_source_file) > $(version_source_file).tmp && $(MV) $(version_source_file).tmp $(version_source_file)
endef

$(bin_targets): %: %.go
	$(call update-version)
	$(QUIET) cd $(dir $@) && $(GO) build -o $(notdir $@) $(notdir $<)

$(gen_dir): $(cloudtable_idl)
	$(QUIET) $(MKDIR) $@ && thrift --gen go -out $@ $<

test: test-root test-generator

test-root:
	$(QUIET) $(GO) test -v

test-generator:
	$(QUIET) cd generator && $(GO) test -v

clean:
	$(QUIET) $(RM) $(gen_dir) $(bin_targets)

