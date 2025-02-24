# Makefile for C XTDB/pq Trade Application

CFLAGS = -Wall -Wextra -g -std=c17  -DTRANSACTION_ISOLATION

PG_CONFIG := $(shell which pg_config 2>/dev/null)
PKG_CONFIG := $(shell which pkg-config 2>/dev/null)

ifdef PG_CONFIG
    PG_CFLAGS := -I$(shell $(PG_CONFIG) --includedir)
    PG_LDFLAGS := -L$(shell $(PG_CONFIG) --libdir) -lpq
else
    ifdef PKG_CONFIG
        PG_CFLAGS := $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
        PG_LDFLAGS := $(shell $(PKG_CONFIG) --libs libpq 2>/dev/null)
    else
        PG_CFLAGS := -I/opt/homebrew/opt/postgresql/include
        PG_LDFLAGS := -L/opt/homebrew/opt/postgresql/lib -lpq
    endif
endif

TARGET = trades
SOURCES = trades.c
OBJECTS = $(SOURCES:.c=.o)
all: $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) $(PG_LDFLAGS) -o $(TARGET)

%.o: %.c
	$(CC) $(CFLAGS) $(PG_CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)

run: $(TARGET)
	./$(TARGET)

debug:
	@echo "PostgreSQL CFLAGS: $(PG_CFLAGS)"
	@echo "PostgreSQL LDFLAGS: $(PG_LDFLAGS)"
	@echo "Compiler: $(CC)"
	@echo "Compiler flags: $(CFLAGS)"
	@echo "pg_config path: $(PG_CONFIG)"
	@echo "pkg-config path: $(PKG_CONFIG)"

.PHONY: all clean run debug help
