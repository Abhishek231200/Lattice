MODULE_big = lattice_smgr
OBJS       = lattice_smgr.o

EXTENSION  = lattice_smgr
DATA       = lattice_smgr--1.0.sql

PG_CONFIG ?= pg_config
PGXS      := $(shell $(PG_CONFIG) --pgxs)

# libcurl for HTTP calls from inside Postgres backends
PG_CPPFLAGS += $(shell curl-config --cflags 2>/dev/null || pkg-config --cflags libcurl 2>/dev/null || echo "")
SHLIB_LINK  += $(shell curl-config --libs  2>/dev/null || pkg-config --libs   libcurl 2>/dev/null || echo "-lcurl")

include $(PGXS)
