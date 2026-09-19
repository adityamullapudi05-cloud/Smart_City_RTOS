/*
 * hal_gpio_driver.c - GPIO hardware abstraction layer driver for Smart City.
 * Provides a portable software simulation of Raspberry Pi GPIO pins and LEDs.
 * Replaces hardware memory-mapped register access with standard POSIX stubs.
 * Key words: 
 *  HAL - Hardware Abstraction Layer
 */

#include "cluster_protocol.h"  /* Include shared types, definitions, and sleep helper */

#define GPIO_LED_GREEN   17     /* Broadcom GPIO pin number assigned to Green LED */
#define GPIO_LED_YELLOW  27     /* Broadcom GPIO pin number assigned to Yellow LED */
#define GPIO_LED_RED     22     /* Broadcom GPIO pin number assigned to Red LED */
#define GPIO_BTN_1       23     /* Broadcom GPIO pin number assigned to Button 1 */
#define GPIO_BTN_2       24     /* Broadcom GPIO pin number assigned to Button 2 */
#define GPIO_BTN_3       25     /* Broadcom GPIO pin number assigned to Button 3 */

#define BCM2711_GPIO_BASE  0xFE200000UL /* Physical base address for BCM2711 GPIO registers */
#define GPIO_REG_SIZE      0x100        /* Byte size of memory-mapped GPIO register block */
#define GPFSEL0   0x00                  /* Offset to GPIO Function Select Register 0 */
#define GPSET0    0x1C                  /* Offset to GPIO Pin Output Set Register 0 */
#define GPCLR0    0x28                  /* Offset to GPIO Pin Output Clear Register 0 */
#define GPLEV0    0x34                  /* Offset to GPIO Pin Level Register 0 */
#define GPPUD     0x94                  /* Offset to GPIO Pin Pull-up/down Register */
#define GPPUDCLK0 0x98                  /* Offset to GPIO Pin Pull-up/down Clock Register 0 */
#define GPPUPPDN0 0xE4                  /* Offset to BCM2711 Pull-up/down Control Register 0 */

typedef void (*gpio_callback_t)(int btn_id, bool pressed); /* Function pointer type for button events */

static bool initialized = false;        /* Internal status flag tracking module initialization */
static int  prev_level[3] = {1, 1, 1};  /* Previous logic levels of the 3 simulated buttons */

static inline void gpio_write(uint32_t off, uint32_t val) { /* Simulated register write function */
    (void)off;                          /* Suppress compiler warning for unused offset parameter */
    (void)val;                          /* Suppress compiler warning for unused value parameter */
}                                       /* Return from register write stub */

static inline uint32_t gpio_read(uint32_t off) { /* Simulated register read function */
    (void)off;                          /* Suppress compiler warning for unused offset parameter */
    return 0;                           /* Return zero as default simulated register value */
}                                       /* Return from register read stub */

static void gpio_set_output(int pin) {  /* Configure simulated pin as output */
    (void)pin;                          /* Suppress unused parameter warning */
}                                       /* Return from pin output configuration */

static void gpio_set_input_pullup(int pin) { /* Configure simulated pin as input with pullup */
    (void)pin;                          /* Suppress unused parameter warning */
}                                       /* Return from pin input configuration */

static void pin_set(int pin) {          /* Set simulated GPIO output pin to logic high */
    (void)pin;                          /* Suppress unused pin parameter in simulation */
}                                       /* Return from pin set function */

static void pin_clr(int pin) {          /* Clear simulated GPIO output pin to logic low */
    (void)pin;                          /* Suppress unused pin parameter in simulation */
}                                       /* Return from pin clear function */

static int pin_get(int pin) {           /* Read simulated logical level of a GPIO pin */
    (void)pin;                          /* Suppress unused pin parameter warning */
    return 1;                           /* Return 1 indicating unpressed button state */
}                                       /* Return from pin read function */

int hal_gpio_init(void) {               /* Initialize simulated GPIO subsystem */
    initialized = true;                 /* Mark module as successfully initialized */
    printf("Hardware abstraction layer initialized (software simulation mode).\n"); /* Log */
    gpio_set_output(GPIO_LED_GREEN);    /* Configure Green LED pin as output */
    gpio_set_output(GPIO_LED_YELLOW);   /* Configure Yellow LED pin as output */
    gpio_set_output(GPIO_LED_RED);      /* Configure Red LED pin as output */
    gpio_set_input_pullup(GPIO_BTN_1);  /* Configure Button 1 pin with pull-up resistor */
    gpio_set_input_pullup(GPIO_BTN_2);  /* Configure Button 2 pin with pull-up resistor */
    gpio_set_input_pullup(GPIO_BTN_3);  /* Configure Button 3 pin with pull-up resistor */
    prev_level[0] = pin_get(GPIO_BTN_1);/* Record initial state for simulated Button 1 */
    prev_level[1] = pin_get(GPIO_BTN_2);/* Record initial state for simulated Button 2 */
    prev_level[2] = pin_get(GPIO_BTN_3);/* Record initial state for simulated Button 3 */
    return 0;                           /* Return zero indicating successful initialization */
}                                       /* End of hal_gpio_init */

void hal_gpio_set_led(HealthLedState state) { /* Set state of the health indicator LED */
    static HealthLedState current_led = (HealthLedState)-1; /* Track previous state */
    if (!initialized) return;           /* Guard against calls prior to initialization */
    if (state == current_led) return;   /* Suppress spam: only act when LED state changes */
    current_led = state;                /* Update recorded LED state */
    pin_clr(GPIO_LED_GREEN);            /* Turn off Green LED output */
    pin_clr(GPIO_LED_YELLOW);           /* Turn off Yellow LED output */
    pin_clr(GPIO_LED_RED);              /* Turn off Red LED output */
    if (state == LED_GREEN)  pin_set(GPIO_LED_GREEN);   /* Turn on Green LED if requested */
    if (state == LED_YELLOW) pin_set(GPIO_LED_YELLOW);  /* Turn on Yellow LED if requested */
    if (state == LED_RED)    pin_set(GPIO_LED_RED);     /* Turn on Red LED if requested */
    printf("Health indicator LED set to %s\n",   /* Print state transition update */
           state == LED_GREEN  ? "GREEN (Healthy)" :    /* Format label for Green LED */
           state == LED_YELLOW ? "YELLOW (Warning)" :   /* Format label for Yellow LED */
           "RED (Critical Fault)");                     /* Format label for Red LED */
}                                       /* End of hal_gpio_set_led */

void *hal_gpio_poll_thread(void *arg) { /* Background polling thread for simulated button inputs */
    gpio_callback_t cb = (gpio_callback_t)arg; /* Extract user-specified event callback function */
    int pins[3] = { GPIO_BTN_1, GPIO_BTN_2, GPIO_BTN_3 }; /* Array of active button pin numbers */
    if (!initialized) return NULL;      /* Guard against running if module is not initialized */
    while (initialized) {               /* Polling loop active while module remains initialized */
        for (int i = 0; i < 3; ++i) {   /* Iterate across all 3 simulated button inputs */
            int level = pin_get(pins[i]); /* Read current logical level of the button pin */
            if (level != prev_level[i]) { /* Detect transition in button signal level */
                if (cb) cb(i, level == 0); /* Invoke callback with button index and pressed state */
                prev_level[i] = level;  /* Update last known level to new state */
            }                           /* End transition check */
        }                               /* End button loop */
        sleep_ms(50);                   /* Sleep for 50ms sampling debounce interval */
    }                                   /* End polling loop */
    return NULL;                        /* Return NULL on thread termination */
}                                       /* End of hal_gpio_poll_thread */

void hal_gpio_deinit(void) {            /* Teardown and deinitialize simulated GPIO subsystem */
    if (!initialized) return;           /* Guard against redundant deinitialization */
    pin_clr(GPIO_LED_GREEN);            /* Turn off Green LED */
    pin_clr(GPIO_LED_YELLOW);           /* Turn off Yellow LED */
    pin_clr(GPIO_LED_RED);              /* Turn off Red LED */
    initialized = false;                /* Mark module as deinitialized */
    printf("Simulation subsystem cleanly deinitialized.\n"); /* Log completion */
}                                       /* End of hal_gpio_deinit */
