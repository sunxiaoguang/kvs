PROJECT_HOME = .
BUILD_DIR ?= $(PROJECT_HOME)/build/make
include $(BUILD_DIR)/make.defs

PRE_BUILD = build 
.PHONY: build

include $(BUILD_DIR)/make.rules

build:
	$(MAKE) -f Makefile.kvs
	$(MAKE) -f Makefile.init
	$(MAKE) -f Makefile.benchmark
	$(MAKE) -f Makefile.select

clean-all:
	$(MAKE) -f Makefile.kvs clean-all
	$(MAKE) -f Makefile.init clean-all
	$(MAKE) -f Makefile.benchmark clean-all
	$(MAKE) -f Makefile.select clean-all
