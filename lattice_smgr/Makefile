MODULE_big = lattice_smgr
OBJS = lattice_smgr.o

EXTENSION = lattice_smgr
DATA = lattice_smgr--1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

# libcurl for HTTP calls to the compute-shim
PG_CPPFLAGS += $(shell curl-config --cflags)
SHLIB_LINK += $(shell curl-config --libs)

include $(PGXS)
