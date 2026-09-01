CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -Werror -static
PKG_CONFIG ?= pkg-config
OBMM_SDK_PKG ?= lingqu-mem-service-obmm-provider
OUT ?= target/uf-ub-sim-agent-obmm

.PHONY: all clean

all: $(OUT)

$(OUT): c/uf_ub_sim_agent_obmm.c
	mkdir -p $(dir $(OUT))
	$(CC) $(CFLAGS) $$(pkg-config --cflags $(OBMM_SDK_PKG)) $< $$(pkg-config --libs --static $(OBMM_SDK_PKG)) -o $@

clean:
	rm -f $(OUT)
