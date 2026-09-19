# ============================================================================
# Makefile — Smart City RTOS Fault & Performance Monitoring Platform
#
# Produces TWO separate executables:
#   build/<platform>-<profile>/node1       → deploy to Raspberry Pi 1
#   build/<platform>-<profile>/supervisor  → deploy to Raspberry Pi 2
#
# Shared source:
#   src/hal_gpio.c           → linked into BOTH binaries
#   src/smart_city_common.h  → header included by both
# ============================================================================

# Build architecture: aarch64le for Raspberry Pi 4/5 on QNX
PLATFORM ?= aarch64le

# Build profile: debug | release | profile | coverage
BUILD_PROFILE ?= debug

CONFIG_NAME ?= $(PLATFORM)-$(BUILD_PROFILE)
OUTPUT_DIR   = build/$(CONFIG_NAME)

# ---- Toolchain ---------------------------------------------------------------
CC  = qcc -Vgcc_nto$(PLATFORM)
CXX = q++ -Vgcc_nto$(PLATFORM)_cxx
LD  = $(CC)

# ---- Compiler flags ----------------------------------------------------------
CCFLAGS_release  += -O2
CCFLAGS_debug    += -g -O0 -fno-builtin
CCFLAGS_coverage += -g -O0 -ftest-coverage -fprofile-arcs
LDFLAGS_coverage += -ftest-coverage -fprofile-arcs
CCFLAGS_profile  += -g -O0 -finstrument-functions
LIBS_profile     += -lprofilingS

CCFLAGS_all += -Wall -fmessage-length=0
CCFLAGS_all += $(CCFLAGS_$(BUILD_PROFILE))
LDFLAGS_all += $(LDFLAGS_$(BUILD_PROFILE))
LIBS_all    += $(LIBS_$(BUILD_PROFILE))

# ---- Required libraries (QNX 8.0) --------------------------------------------
#   -lsocket   : BSD sockets (TCP fallback when Qnet name_open is absent)
#   -lm        : sin() in the CPU-overload burner
#   -lslog2    : slog2_register / slog2c  (read with slog2info)
LIBS += -lsocket -lm -lslog2

DEPS = -Wp,-MMD,$(@:%.o=%.d),-MT,$@

# ---- Source → Object mapping -------------------------------------------------
# Shared object compiled once, reused by both binaries
HAL_OBJ   = $(OUTPUT_DIR)/src/hal_gpio_driver.o
NODE1_OBJ = $(OUTPUT_DIR)/src/edge_workload_node.o
NODE2_OBJ = $(OUTPUT_DIR)/src/control_supervisor.o

# ---- Final binary targets ----------------------------------------------------
TARGET_NODE1 = $(OUTPUT_DIR)/node1
TARGET_NODE2 = $(OUTPUT_DIR)/supervisor

# ---- Default target: build both binaries ------------------------------------
all: $(TARGET_NODE1) $(TARGET_NODE2)

# ---- Compile rule (generic .c → .o) -----------------------------------------
$(OUTPUT_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) -c $(DEPS) -o $@ $(INCLUDES) $(CCFLAGS_all) $(CCFLAGS) $<

# ---- Link: Node 1 (Workload) ------------------------------------------------
#   Sources: node1.c + hal_gpio.c
$(TARGET_NODE1): $(NODE1_OBJ) $(HAL_OBJ)
	$(LD) -o $@ $(LDFLAGS_all) $(LDFLAGS) $^ $(LIBS_all) $(LIBS)
	@echo ""
	@echo "  [OK] node1  -> deploy to Raspberry Pi 1"
	@echo "       Run: ./node1 --ip=<Pi2_IP>"

# ---- Link: Node 2 (Supervisor) ----------------------------------------------
#   Sources: supervisor.c + hal_gpio.c
$(TARGET_NODE2): $(NODE2_OBJ) $(HAL_OBJ)
	$(LD) -o $@ $(LDFLAGS_all) $(LDFLAGS) $^ $(LIBS_all) $(LIBS)
	@echo ""
	@echo "  [OK] supervisor -> deploy to Raspberry Pi 2"
	@echo "       Run: ./supervisor"

# ---- Utility targets --------------------------------------------------------
clean:
	rm -fr $(OUTPUT_DIR)

rebuild: clean all

# ---- Dependency inclusion ---------------------------------------------------
-include $(HAL_OBJ:%.o=%.d)
-include $(NODE1_OBJ:%.o=%.d)
-include $(NODE2_OBJ:%.o=%.d)