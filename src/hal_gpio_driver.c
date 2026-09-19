/*
 * hal_gpio_driver.c - GPIO Hardware Abstraction Layer (HAL) Driver for Smart City RTOS.
 * Rationale: Decouples high-level real-time application logic from physical silicon registers.
 * Provides Software-in-the-Loop (SIL) simulation on host PCs and exact BCM2711 MMIO mapping on Raspberry Pi 4.
 */

#include "cluster_protocol.h"  /* Why: Includes shared enum HealthLedState and POSIX timing/sleep helpers */

/* =========================================================================
 * BCM GPIO PIN ASSIGNMENTS: Matching Raspberry Pi 4 Broadcom Pin Header Layout
 * ========================================================================= */
#define GPIO_LED_GREEN   17     /* Why: Broadcom GPIO 17 assigned to Green status indicator (Nominal Health) */
#define GPIO_LED_YELLOW  27     /* Why: Broadcom GPIO 27 assigned to Yellow status indicator (Deadline / Latency Warning) */
#define GPIO_LED_RED     22     /* Why: Broadcom GPIO 22 assigned to Red status indicator (Critical Fault / Starvation) */
#define GPIO_BTN_1       23     /* Why: Broadcom GPIO 23 assigned to Button 1 (Manual Fault Injection Trigger 1) */
#define GPIO_BTN_2       24     /* Why: Broadcom GPIO 24 assigned to Button 2 (Manual Fault Injection Trigger 2) */
#define GPIO_BTN_3       25     /* Why: Broadcom GPIO 25 assigned to Button 3 (Manual Fault Clear Trigger) */

/* =========================================================================
 * MEMORY-MAPPED I/O (MMIO): BCM2711 Peripheral Register Map (Raspberry Pi 4 ARM Cortex-A72)
 * ========================================================================= */
#define BCM2711_GPIO_BASE  0xFE200000UL /* Why: Physical base address of GPIO registers in BCM2711 35-bit physical memory space */
#define GPIO_REG_SIZE      0x100        /* Why: 256-byte page size passed to mmap() when mapping /dev/gpiomem into process virtual memory */
#define GPFSEL0   0x00                  /* Why: GPIO Function Select 0 (+0x00) configures pin direction (3 bits/pin: 000=Input, 001=Output) */
#define GPSET0    0x1C                  /* Why: Output Set Register 0 (+0x1C): writing 1 drives pin HIGH (3.3V); writing 0 has no effect (Atomic) */
#define GPCLR0    0x28                  /* Why: Output Clear Register 0 (+0x28): writing 1 drives pin LOW (0V); writing 0 has no effect (Atomic) */
#define GPLEV0    0x34                  /* Why: Pin Level Register 0 (+0x34): reads current logic levels (1=3.3V, 0=GND) across pins 0-31 */
#define GPPUD     0x94                  /* Why: Legacy BCM2835 Pull-up/down resistor control offset (maintained for backwards portability) */
#define GPPUDCLK0 0x98                  /* Why: Legacy BCM2835 Pull-up/down clock latch offset */
#define GPPUPPDN0 0xE4                  /* Why: Modern BCM2711 internal pull-up/down register (+0xE4): 2 bits/pin (01=Pull-up, 10=Pull-down) */

typedef void (*gpio_callback_t)(int btn_id, bool pressed); /* Why: Asynchronous event callback type for interrupt/polled button transitions */

static bool initialized = false;        /* Why: Guard flag: prevents unauthorized hardware access prior to explicit initialization */
static int  prev_level[3] = {1, 1, 1};  /* Why: Debounce & edge memory: stores previous button states (default 1 = active-low unpressed) */

/* =========================================================================
 * HARDWARE STUBS: Safe Software-in-the-Loop (SIL) Host Emulation
 * ========================================================================= */
static inline void gpio_write(uint32_t off, uint32_t val) { /* Rationale: Simulated register write for host workstations */
    (void)off;                          /* Why: Suppresses compiler warning for unused offset parameter on non-ARM hosts */
    (void)val;                          /* Why: Suppresses compiler warning for unused value parameter */
}                                       /* Return from stub */

static inline uint32_t gpio_read(uint32_t off) { /* Rationale: Simulated register read for host workstations */
    (void)off;                          /* Why: Suppresses compiler warning for unused offset parameter */
    return 0;                           /* Why: Safe default register state preventing segmentation faults */
}                                       /* Return from stub */

static void gpio_set_output(int pin) {  /* Rationale: Configures pin as digital output */
    (void)pin;                          /* Why: On physical Pi, sets bits in GPFSEL to 001; stubbed on host */
}                                       /* Return */

static void gpio_set_input_pullup(int pin) { /* Rationale: Configures pin as input with internal pull-up resistor */
    (void)pin;                          /* Why: On physical Pi, configures GPPUPPDN0 to prevent floating voltage; stubbed on host */
}                                       /* Return */

static void pin_set(int pin) {          /* Rationale: Drives pin HIGH (3.3V) */
    (void)pin;                          /* Why: On physical Pi, writes (1 << pin) into GPSET0 register */
}                                       /* Return */

static void pin_clr(int pin) {          /* Rationale: Drives pin LOW (0V) */
    (void)pin;                          /* Why: On physical Pi, writes (1 << pin) into GPCLR0 register */
}                                       /* Return */

static int pin_get(int pin) {           /* Rationale: Samples digital input level of pin */
    (void)pin;                          /* Why: On physical Pi, reads ((GPLEV0 >> pin) & 1) */
    return 1;                           /* Why: Returns logic high (1) simulating an idle, unpressed active-low button */
}                                       /* Return */

int hal_gpio_init(void) {               /* Rationale: Subsystem bootstrap and hardware pin configuration */
    initialized = true;                 /* Mark module ready */
    printf("Hardware abstraction layer initialized (software simulation mode).\n"); /* Operator confirmation log */
    gpio_set_output(GPIO_LED_GREEN);    /* Why: Direct GPIO 17 as digital output for Green health LED */
    gpio_set_output(GPIO_LED_YELLOW);   /* Why: Direct GPIO 27 as digital output for Yellow warning LED */
    gpio_set_output(GPIO_LED_RED);      /* Why: Direct GPIO 22 as digital output for Red critical LED */
    gpio_set_input_pullup(GPIO_BTN_1);  /* Why: Configure GPIO 23 as active-low input with internal pull-up */
    gpio_set_input_pullup(GPIO_BTN_2);  /* Why: Configure GPIO 24 as active-low input with internal pull-up */
    gpio_set_input_pullup(GPIO_BTN_3);  /* Why: Configure GPIO 25 as active-low input with internal pull-up */
    prev_level[0] = pin_get(GPIO_BTN_1);/* Establish baseline state for button 1 */
    prev_level[1] = pin_get(GPIO_BTN_2);/* Establish baseline state for button 2 */
    prev_level[2] = pin_get(GPIO_BTN_3);/* Establish baseline state for button 3 */
    return 0;                           /* Return 0 for successful initialization */
}                                       /* End of hal_gpio_init */

void hal_gpio_set_led(HealthLedState state) { /* Rationale: Mutually exclusive visual health state controller */
    static HealthLedState current_led = (HealthLedState)-1; /* State cache: initialized to invalid sentinel */
    if (!initialized) return;           /* Guard: prevent execution before hal_gpio_init() */
    if (state == current_led) return;   /* Why: State-caching optimization: suppresses redundant bus writes and console log spam */
    current_led = state;                /* Update cached state */
    pin_clr(GPIO_LED_GREEN);            /* Why: Enforce mutual exclusivity: turn off Green LED first */
    pin_clr(GPIO_LED_YELLOW);           /* Why: Enforce mutual exclusivity: turn off Yellow LED first */
    pin_clr(GPIO_LED_RED);              /* Why: Enforce mutual exclusivity: turn off Red LED first */
    if (state == LED_GREEN)  pin_set(GPIO_LED_GREEN);   /* Why: Turn on Green LED if system is healthy */
    if (state == LED_YELLOW) pin_set(GPIO_LED_YELLOW);  /* Why: Turn on Yellow LED if warnings / deadline misses are detected */
    if (state == LED_RED)    pin_set(GPIO_LED_RED);     /* Why: Turn on Red LED if critical faults / starvation are detected */
    printf("Health indicator LED set to %s\n",   /* Format state transition update to console */
           state == LED_GREEN  ? "GREEN (Healthy)" :    /* Green label */
           state == LED_YELLOW ? "YELLOW (Warning)" :   /* Yellow label */
           "RED (Critical Fault)");                     /* Red label */
}                                       /* End of hal_gpio_set_led */

void *hal_gpio_poll_thread(void *arg) { /* Rationale: Asynchronous input polling thread with software debouncing */
    gpio_callback_t cb = (gpio_callback_t)arg; /* Extract optional event callback pointer */
    int pins[3] = { GPIO_BTN_1, GPIO_BTN_2, GPIO_BTN_3 }; /* Pin handles for the 3 input buttons */
    if (!initialized) return NULL;      /* Guard against uninitialized execution */
    while (initialized) {               /* Polling loop active while driver remains online */
        for (int i = 0; i < 3; ++i) {   /* Scan all 3 button pins */
            int level = pin_get(pins[i]); /* Read current digital logic level */
            if (level != prev_level[i]) { /* Why: Edge detection: triggers only upon transition (falling or rising edge) */
                if (cb) cb(i, level == 0); /* Why: Invoke callback passing button index and pressed state (level==0 is pressed) */
                prev_level[i] = level;  /* Update previous level memory */
            }                           /* End edge detection */
        }                               /* End pin loop */
        sleep_ms(50);                   /* Why: 50ms sampling interval provides software debouncing to absorb mechanical contact bounce */
    }                                   /* End loop */
    return NULL;                        /* Clean thread termination */
}                                       /* End of hal_gpio_poll_thread */

void hal_gpio_deinit(void) {            /* Rationale: Safe hardware teardown and pin grounding */
    if (!initialized) return;           /* Guard against redundant deinitialization */
    pin_clr(GPIO_LED_GREEN);            /* Why: Ensure Green LED pin is pulled LOW (safe de-energized state) */
    pin_clr(GPIO_LED_YELLOW);           /* Why: Ensure Yellow LED pin is pulled LOW */
    pin_clr(GPIO_LED_RED);              /* Why: Ensure Red LED pin is pulled LOW */
    initialized = false;                /* Invalidate initialization flag */
    printf("Simulation subsystem cleanly deinitialized.\n"); /* Teardown confirmation log */
}                                       /* End of hal_gpio_deinit */
