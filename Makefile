# Makefile for yabf

QUIET    := @
MKDIR    := mkdir -p
RM       := rm -rf
SED      := sed
GIT      := git
GO       := go

root_dir := $(CURDIR)
bin_dir  := ./main
bin_targets := $(patsubst %.go,%,$(wildcard $(bin_dir)/*.go))
version_file := version.go
version_source_file := $(root_dir)/$(version_file)
source_files := $(filter-out %$(version_file),$(filter-out %test.go,$(shell find $(root_dir) -name '*.go')))


.PHONY: all gen test test-root test-generator clean

all: $(bin_targets)

$(bin_targets): $(source_files)

# write git latest version to version file
# $(call update-version)
define update-version
  $(QUIET) $(SED) -e "s/\\(.*\"\\)[^\"]*\\(\".*\\)/\\1$$($(GIT) rev-parse --short HEAD)\\2/g" < $(version_source_file).template > $(version_source_file)
endef

$(bin_targets): %: %.go
	$(call update-version)
	$(QUIET) cd $(dir $@) && $(GO) build -o $(notdir $@) $(notdir $<)

test: test-root test-generator

test-root:
	$(QUIET) $(GO) test -v

test-generator:
	$(QUIET) cd generator && $(GO) test -v

clean:
	$(QUIET) $(RM) $(bin_targets)

