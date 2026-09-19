/*
 * cluster_protocol.h - Common definitions, structures, and utilities.
 *
 * Rationale:
 * This header establishes the shared architectural contract between Edge Workload Nodes (Node 1)
 * and the Control Supervisor (Node 2). It provides:
 * 1. Hard real-time priority schedules according to Rate-Monotonic Analysis (RMA).
 * 2. Anomaly detection thresholds for CPU overload and IPC latency degradation.
 * 3. Self-contained binary packet specifications for zero-overhead telemetry transfer over TCP.
 * 4. A portable RTOS abstraction layer mapping POSIX threads and mutexes to real-time primitives.
 * 5. High-resolution monotonic timing routines and statistical percentile calculation algorithms.
 */

#ifndef CLUSTER_PROTOCOL_H  /* Header guard to prevent multiple inclusion */
#define CLUSTER_PROTOCOL_H  /* Define header guard symbol */

#include <stdio.h>       /* Standard input/output library for printf and snprintf */
#include <stdlib.h>      /* Standard general utilities for memory, exit, and atoi */
#include <stdint.h>      /* Exact-width integer types such as uint32_t and uint64_t */
#include <stdbool.h>     /* Boolean type definitions (true and false) */
#include <string.h>      /* String manipulation functions like memset, memcpy, strncpy */
#include <time.h>        /* Time manipulation functions like clock_gettime and nanosleep */
#include <math.h>        /* Mathematical functions like sin, used in cpu burner */
#include <inttypes.h>    /* Format specifier macros for exact-width integer printing */
#include <errno.h>       /* System error number definitions for error checking */
#include <pthread.h>     /* POSIX threads, mutexes, and condition variables */
#include <unistd.h>      /* Standard symbolic constants and types (e.g. close, sleep) */
#include <signal.h>      /* Signal handling functions such as signal(SIGPIPE, ...) */
#include <fcntl.h>       /* File control options like fcntl, O_NONBLOCK */
#include <sys/socket.h>  /* Core socket structures and functions for TCP networking */
#include <sys/select.h>  /* Synchronous I/O multiplexing via select */
#include <netinet/in.h>  /* Internet address family and sockaddr_in structure */
#include <arpa/inet.h>   /* Definitions for internet operations like inet_addr */

#ifndef EOK              /* Check if EOK error code is not defined */
#define EOK 0            /* Define EOK as 0 (standard success return code) */
#endif                   /* End of EOK check */

#define FMT_U64 "%" PRIu64  /* Macro for printing 64-bit unsigned integers portably */

/*
 * Rationale: System Operational and Buffer Sizing Constants
 */
#define PLATFORM_NAME          "Smart City RTOS Monitor"  /* Application platform title string */
#define TELEMETRY_PORT         5555                       /* Default TCP port for telemetry transfer */
#define TELEMETRY_INTERVAL_MS  500                        /* Telemetry publishing period: 2 Hz network refresh rate */
#define MONITOR_INTERVAL_MS    100                        /* Local watchdog check period: 10 Hz health audit rate */
#define MAX_TASKS              3                          /* Number of simulated real-time city service tasks */
#define TRACE_BUFFER_SIZE      64                         /* Ring buffer depth: stores last 64 events for forensics */
#define IPC_HISTORY_SIZE       50                         /* Rolling window size for P95/Avg IPC latency metrics */
#define MAX_ACTIVE_FAULTS      16                         /* Fault registry capacity: prevents unbounded memory growth */

/*
 * Rationale: Rate-Monotonic Priority Assignment Hierarchy
 *
 * Why: In fixed-priority preemptive scheduling (Rate-Monotonic Analysis):
 * 1. Supervisory and watchdog tasks hold the HIGHEST priorities (25, 20) so they can detect
 *    and report runaway tasks even if the CPU is congested.
 * 2. Workload tasks follow Liu & Layland's Theorem: shorter period (T) => higher priority.
 *    - Service A (Traffic, T=100ms) -> Priority 15
 *    - Service B (Power,   T=200ms) -> Priority 14
 *    - Service C (Env,     T=500ms) -> Priority 13
 * 3. Network telemetry, GPIO polling, and CLI run at LOW priorities (10, 8, 5) so background
 *    I/O and user interaction never delay safety-critical real-time services.
 */
#define PRIORITY_FAULT_DETECTOR 25  /* Priority level for fault detector thread (Highest) */
#define PRIORITY_MONITOR        20  /* Priority level for local monitor watchdog thread */
#define PRIORITY_SERVICE_A      15  /* Priority level for Service A (Traffic Signal: T=100ms) */
#define PRIORITY_SERVICE_B      14  /* Priority level for Service B (Power Grid: T=200ms) */
#define PRIORITY_SERVICE_C      13  /* Priority level for Service C (Environment Sensor: T=500ms) */
#define PRIORITY_TELEMETRY      10  /* Priority level for network telemetry worker */
#define PRIORITY_GPIO            8  /* Priority level for simulated GPIO polling thread */
#define PRIORITY_CLI             5  /* Priority level for diagnostic command line interface */

/*
 * Rationale: Supervisory Anomaly Detection Thresholds
 *
 * Why:
 * - CPU_WARN_THRESHOLD (70%): Approaching the theoretical Rate-Monotonic bound for 3 tasks
 *   (U = 3*(2^(1/3) - 1) ~= 77.98%). Exceeding 70% warns of approaching scheduler instability.
 * - CPU_CRIT_THRESHOLD (85%): Hard saturation limit where lower priority tasks face guaranteed starvation.
 * - IPC_WARN_LATENCY_US (220 ms): Slower than Service B's period (200 ms), indicating communication backpressure.
 * - IPC_CRIT_LATENCY_US (300 ms): Indicates severe transport congestion or lock contention.
 */
#define CPU_WARN_THRESHOLD      70.0f    /* Warning threshold for total CPU usage percentage */
#define CPU_CRIT_THRESHOLD      85.0f    /* Critical threshold for total CPU usage percentage */
#define IPC_WARN_LATENCY_US     220000U  /* Warning threshold for IPC message latency in microseconds (220 ms) */
#define IPC_CRIT_LATENCY_US     300000U  /* Critical threshold for IPC message latency in microseconds (300 ms) */

/*
 * Rationale: Task State Lifecycle Enumeration
 * Tracks operational readiness of each real-time thread.
 */
typedef enum {
    TASK_STATE_RUNNING = 0,  /* Task is executing periodically and meeting all deadlines */
    TASK_STATE_WARNING,      /* Task missed one heartbeat or execution time is close to deadline */
    TASK_STATE_STARVED,      /* Task missed >= 3 consecutive execution periods without heartbeat */
    TASK_STATE_STOPPED       /* Task has exited or been gracefully terminated */
} TaskState;

/*
 * Rationale: Fault Classification Enumeration
 * Classifies anomalies detected by local watchdog or remote supervisor.
 */
typedef enum {
    FAULT_NONE = 0,               /* Nominal system state, no anomalies present */
    FAULT_TASK_STARVATION,        /* Task failed to emit heartbeat within expected period multiple */
    FAULT_DEADLINE_MISS,          /* Task execution duration (exec_us) exceeded hard deadline (D_i) */
    FAULT_CPU_OVERLOAD,           /* Aggregate processor utilization exceeded safe thresholds */
    FAULT_IPC_TIMEOUT,            /* Inter-task communication round-trip latency exceeded SLA bounds */
    FAULT_NODE_DISCONNECTED       /* TCP telemetry stream interrupted or dropped */
} FaultType;

/*
 * Rationale: Fault Severity Ranking
 * Maps system degradation level to visual indicators and diagnostic alerts.
 */
typedef enum {
    SEV_INFO = 0,      /* Informational message with no real-time impact */
    SEV_WARNING,       /* Non-fatal degradation; triggers YELLOW warning indicator */
    SEV_CRITICAL       /* Hard real-time violation or network loss; triggers RED critical indicator */
} FaultSeverity;

/*
 * Rationale: Physical/Simulated Health Status LED States
 * Reflected directly on hardware GPIO pins (BCM 17, 27, 22) or terminal HUD.
 */
typedef enum {
    LED_GREEN = 0, /* System operates nominally: all deadlines met, CPU < 70%, 0 active faults */
    LED_YELLOW,    /* Warning state: CPU 70-85%, IPC latency elevated, or minor heartbeat delays */
    LED_RED        /* Critical failure: deadline miss, task starvation, CPU >= 85%, or node disconnect */
} HealthLedState;

/*
 * Rationale: Execution Trace Event Classification
 * Categorizes microsecond-timestamped events recorded in the circular trace buffer.
 */
typedef enum {
    EVENT_TASK_START = 1, /* Task acquired execution slot and began processing */
    EVENT_TASK_END,       /* Task finished processing cycle and verified deadline compliance */
    EVENT_HEARTBEAT,      /* Task emitted periodic liveness pulse to health monitor */
    EVENT_FAULT,          /* Watchdog or task triggered an active fault condition */
    EVENT_IPC_XFER        /* Inter-service message exchange completed with recorded latency */
} TraceEventType;

/*
 * Rationale: Remote Operator Command Classification
 * Commands sent from Supervisor to Node 1 via piggybacked TCP ACKs to test fault tolerance.
 */
typedef enum {
    CMD_NONE = 0,          /* No command pending (idle acknowledgment) */
    CMD_CLEAR_ALL,         /* Clear all active faults, reset LED to GREEN, clear artificial delays */
    CMD_INJECT_STARVE,     /* Suppress target task heartbeats to test starvation detection */
    CMD_INJECT_DEADLINE,   /* Inject artificial busy-wait delay to force hard deadline misses */
    CMD_INJECT_CPU,        /* Turn synthetic CPU burner thread on/off to test overload alarms */
    CMD_INJECT_IPC         /* Inject artificial transmission latency into IPC data transfers */
} ControlCmdType;

/*
 * Rationale: Task Control Block (TCB) Structure
 * Contains all real-time scheduling metadata, execution statistics, and injection flags for a thread.
 */
typedef struct {
    /* Why: task_id provides unique numeric index (1 to MAX_TASKS) for quick array addressing */
    uint32_t  task_id;
    /* Why: name provides human-readable label (e.g., "Service_A (Traffic)") for CLI and logs */
    char      name[32];
    /* Why: period_ms defines recurrence interval (T_i) used for Rate-Monotonic scheduling */
    uint32_t  period_ms;
    /* Why: deadline_ms defines relative deadline (D_i); must satisfy D_i <= T_i */
    uint32_t  deadline_ms;
    /* Why: priority defines POSIX thread priority assigned according to RMA */
    uint32_t  priority;
    /* Why: heartbeat is an incremental pulse counter tracking cumulative task liveness */
    uint32_t  heartbeat;
    /* Why: last_heartbeat_time_us stores monotonic timestamp used to calculate heartbeat age */
    uint64_t  last_heartbeat_time_us;
    /* Why: last_exec_us stores exact microsecond execution time of most recent cycle */
    uint32_t  last_exec_us;
    /* Why: max_exec_us tracks Worst-Case Execution Time (WCET) across all cycles */
    uint32_t  max_exec_us;
    /* Why: total_cycles counts total completed iterations for duty cycle auditing */
    uint32_t  total_cycles;
    /* Why: deadline_misses audits how many times last_exec_us exceeded deadline_ms * 1000 */
    uint32_t  deadline_misses;
    /* Why: cpu_usage_pct calculates individual task duty cycle: (exec_us / (period_ms * 1000)) * 100% */
    float     cpu_usage_pct;
    /* Why: state indicates current operational status (Running, Warning, Starved) */
    TaskState state;
    /* Why: inject_starvation simulates deadlocks or infinite loops by withholding heartbeats */
    bool      inject_starvation;
    /* Why: inject_exec_delay_ms forces artificial busy-waiting to verify deadline miss detection */
    uint32_t  inject_exec_delay_ms;
} TaskControlBlock;

/*
 * Rationale: Fault Record Structure
 * Represents a discrete anomaly registered in the system fault table.
 */
typedef struct {
    /* Why: fault_id uniquely identifies the fault occurrence in chronological order */
    uint32_t      fault_id;
    /* Why: node_id distinguishes whether fault originated on Node 1 (Edge) or Node 2 (Supervisor) */
    uint32_t      node_id;
    /* Why: task_id identifies which service failed (1-3), or 0 for node-wide anomalies */
    uint32_t      task_id;
    /* Why: type categorizes failure mode (Starvation, Deadline Miss, CPU, IPC, Disconnect) */
    FaultType     type;
    /* Why: severity drives LED status transitions (Warning -> Yellow, Critical -> Red) */
    FaultSeverity severity;
    /* Why: timestamp_us records exact monotonic moment the anomaly was identified */
    uint64_t      timestamp_us;
    /* Why: description provides human-readable diagnostic message for the CLI */
    char          description[64];
    /* Why: active boolean indicates whether the fault condition is currently ongoing */
    bool          active;
} FaultRecord;

/*
 * Rationale: IPC Latency Statistics Structure
 * Tracks inter-task communication latency using a rolling circular buffer and statistical percentiles.
 */
typedef struct {
    /* Why: samples array maintains rolling circular history of recent transmission latencies */
    uint32_t samples[IPC_HISTORY_SIZE];
    /* Why: count tracks number of valid samples currently populated in buffer */
    uint32_t count;
    /* Why: head points to array index where next incoming latency sample will be written */
    uint32_t head;
    /* Why: min_us records lowest observed latency in active sample window */
    uint32_t min_us;
    /* Why: max_us records peak observed latency in active sample window */
    uint32_t max_us;
    /* Why: avg_us records arithmetic mean latency across active window */
    uint32_t avg_us;
    /* Why: p95_us records 95th percentile latency (tail latency) using insertion sort */
    uint32_t p95_us;
} IpcStats;

/*
 * Rationale: Lightweight Execution Trace Record Structure
 * Captures discrete real-time events for microsecond-resolution post-mortem timeline reconstruction.
 */
typedef struct {
    /* Why: timestamp_us provides high-resolution timestamp of event occurrence */
    uint64_t       timestamp_us;
    /* Why: task_id specifies which real-time thread triggered the trace event */
    uint32_t       task_id;
    /* Why: event_type indicates whether event was task start, end, heartbeat, fault, or IPC */
    TraceEventType event_type;
    /* Why: duration_us records execution duration or transfer latency if applicable */
    uint32_t       duration_us;
} TraceRecord;

/*
 * Rationale: Trace Ring Buffer Structure
 * Fixed-size circular array storing recent operational trace records without dynamic allocations.
 */
typedef struct {
    /* Why: records array is pre-allocated to avoid heap allocation jitter during runtime */
    TraceRecord records[TRACE_BUFFER_SIZE];
    /* Why: head indicates next write index, wrapping around via modulo arithmetic */
    uint32_t    head;
    /* Why: count tracks total active events stored up to TRACE_BUFFER_SIZE */
    uint32_t    count;
} TraceRingBuffer;

/* Message Type Identifiers */
#define SC_MSG_TELEMETRY   1  /* Periodic telemetry packet streamed from Node 1 to Supervisor */
#define SC_MSG_HEARTBEAT   2  /* Task heartbeat pulse message */
#define SC_MSG_IPC_TRAFFIC 3  /* Inter-service data exchange payload message */

/*
 * Rationale: Control Command Packet Structure
 * Piggybacked in Supervisor TCP acknowledgments to remotely trigger diagnostics or fault injection.
 */
typedef struct {
    /* Why: type specifies command action (CLEAR_ALL, INJECT_STARVE, INJECT_DEADLINE, etc.) */
    ControlCmdType type;
    /* Why: task_id targets specific task (1-3) or 0 for global application actions */
    uint32_t       task_id;
    /* Why: value passes integer argument (e.g., millisecond delay or boolean 1/0 enable) */
    uint32_t       value;
} ControlCmd;

/*
 * Rationale: Comprehensive Telemetry Packet Structure
 * Self-contained binary payload streamed periodically over TCP from Node 1 to Supervisor.
 * Transmits CPU utilization, health LED state, IPC latency, TCB snapshots, active faults, and traces.
 */
typedef struct {
    uint16_t         type;                       /* Packet identifier (SC_MSG_TELEMETRY) */
    uint32_t         node_id;                    /* Originating node ID (1 for Edge Node) */
    uint64_t         timestamp_us;               /* Microsecond timestamp of transmission */
    float            cpu;                        /* Current aggregate CPU utilization percentage */
    int32_t          led;                        /* Current health status LED color state */
    IpcStats         ipc;                        /* Complete IPC latency statistics window */
    TaskControlBlock tasks[MAX_TASKS];           /* Current TCB snapshots for all 3 real-time tasks */
    FaultRecord      fault_map[MAX_ACTIVE_FAULTS]; /* Active and recent fault records */
    uint32_t         fault_counter;              /* Lifetime total fault count observed */
    TraceRingBuffer  trace_buf;                  /* Circular ring buffer of recent trace events */
    bool             inject_cpu_overload;        /* Current status of CPU burner injection */
    uint32_t         ipc_inject_delay_ms;        /* Current injected IPC artificial delay */
} TelemetryMsg;

/*
 * Rationale: Telemetry Acknowledgment Structure
 * Returned by Supervisor to confirm telemetry receipt and dispatch pending operator commands.
 */
typedef struct {
    uint16_t   status;    /* Acknowledgment return code (EOK on success) */
    ControlCmd cmd;       /* Piggybacked remote control command for Node 1 to execute */
} TelemetryAck;

/*
 * Rationale: Intra-Node Task Heartbeat Packet
 */
typedef struct {
    uint16_t type;          /* Message type (SC_MSG_HEARTBEAT) */
    uint32_t task_id;       /* ID of the reporting task */
    uint64_t timestamp_us;  /* Monotonic timestamp of the heartbeat event */
} HeartbeatMsg;

typedef struct {
    uint16_t status;        /* Heartbeat reply status code */
} HeartbeatReply;

/*
 * Rationale: Inter-Service Data Transmission Packet
 * Used for simulated intra-node communication between Service A and Service B.
 */
typedef struct {
    uint16_t type;          /* Message type identifier (SC_MSG_IPC_TRAFFIC) */
    uint32_t sequence;      /* Incremental sequence counter to detect dropped packets */
    uint64_t send_time_us;  /* Monotonic timestamp when sender dispatched message */
    char     payload[64];   /* Character payload containing sensor or control telemetry */
} IpcTrafficMsg;

typedef struct {
    uint16_t status;        /* Response status code */
    uint64_t recv_time_us;  /* Monotonic timestamp when receiver received the message */
} IpcTrafficReply;

/*
 * Rationale: RTOS Abstraction Layer (Mapping to POSIX)
 *
 * Why: Provides standard RTOS-style types (rt_thread_t, rt_mutex_t, rt_cond_t)
 * implemented via POSIX primitives to allow seamless compilation on Linux/QNX/Raspberry Pi.
 */
typedef pthread_t       rt_thread_t;  /* Alias pthread_t to rt_thread_t for standard POSIX threading */
typedef pthread_mutex_t rt_mutex_t;   /* Alias pthread_mutex_t to rt_mutex_t for mutex locks */
typedef pthread_cond_t  rt_cond_t;    /* Alias pthread_cond_t to rt_cond_t for condition variables */

#define RT_MUTEX_INIT(m)    pthread_mutex_init((m), NULL)     /* Initialize standard POSIX mutex */
#define RT_MUTEX_DESTROY(m) pthread_mutex_destroy((m))        /* Destroy allocated POSIX mutex */
#define RT_MUTEX_LOCK(m)    pthread_mutex_lock((m))           /* Acquire lock on POSIX mutex */
#define RT_MUTEX_UNLOCK(m)  pthread_mutex_unlock((m))         /* Release lock on POSIX mutex */
#define RT_COND_INIT(c)     pthread_cond_init((c), NULL)      /* Initialize POSIX condition variable */
#define RT_COND_SIGNAL(c)   pthread_cond_signal((c))          /* Wake one thread waiting on condition */
#define RT_COND_WAIT(c, m)  pthread_cond_wait((c), (m))       /* Block thread on condition with mutex */

/*
 * Rationale: get_time_us() provides monotonic microsecond timestamps.
 *
 * Why: Uses CLOCK_MONOTONIC instead of CLOCK_REALTIME to ensure time never jumps backwards
 * due to NTP adjustments or timezone changes, which is critical for real-time deadline calculations.
 */
static inline uint64_t get_time_us(void) {           /* Retrieve monotonic timestamp in microseconds */
    struct timespec ts;                              /* Allocate timespec structure on the stack */
    clock_gettime(CLOCK_MONOTONIC, &ts);             /* Fetch current time using monotonic clock */
    return ((uint64_t)ts.tv_sec * 1000000ULL) +      /* Multiply whole seconds by 1,000,000 to get us */
           ((uint64_t)ts.tv_nsec / 1000ULL);         /* Divide fractional nanoseconds by 1,000 to get us */
}                                                    /* Return calculated timestamp */

/*
 * Rationale: get_clock_cycles() provides nanosecond monotonic resolution.
 */
static inline uint64_t get_clock_cycles(void) {      /* Monotonic time representation for cycle counters */
    struct timespec ts;                              /* Allocate timespec structure on the stack */
    clock_gettime(CLOCK_MONOTONIC, &ts);             /* Read monotonic system clock */
    return ((uint64_t)ts.tv_sec * 1000000000ULL) +   /* Multiply seconds to obtain nanosecond count */
           (uint64_t)ts.tv_nsec;                     /* Add residual nanoseconds to total */
}                                                    /* Return total monotonic nanoseconds */

/*
 * Rationale: sleep_ms() provides high-resolution thread suspension.
 *
 * Why: Uses nanosleep() to allow the OS scheduler to preempt the calling thread cleanly
 * without busy-waiting, releasing CPU cycles to lower-priority threads.
 */
static inline void sleep_ms(uint32_t ms) {           /* Sleep execution for specified milliseconds */
    struct timespec req;                             /* Allocate request timespec structure */
    req.tv_sec  = (time_t)(ms / 1000);               /* Extract full seconds portion */
    req.tv_nsec = (long)(ms % 1000) * 1000000L;      /* Convert remaining milliseconds to nanoseconds */
    nanosleep(&req, NULL);                           /* Suspend thread execution via nanosleep */
}                                                    /* Finish sleep function */

/*
 * Rationale: rt_thread_create() wraps POSIX thread creation.
 */
static inline int rt_thread_create(rt_thread_t *t, int pri, void *(*fn)(void *), void *arg) { /* Create thread */
    (void)pri;                                       /* Ignore priority parameter on generic POSIX targets */
    return pthread_create(t, NULL, fn, arg);         /* Launch new POSIX thread with default attributes */
}                                                    /* Return thread creation return code */

/*
 * Rationale: rt_thread_join() blocks until the target thread completes execution.
 */
static inline void rt_thread_join(rt_thread_t t) {   /* Wait for specified thread to terminate */
    pthread_join(t, NULL);                           /* Block until target thread exits */
}                                                    /* Return from thread join */

/*
 * Rationale: ipc_add_sample() inserts an IPC latency measurement into the rolling window
 * and computes statistical metrics: Minimum, Maximum, Arithmetic Mean, and 95th Percentile (P95).
 *
 * Why:
 * 1. Rolling circular buffer (IPC_HISTORY_SIZE = 50) caps memory usage and provides temporal locality.
 * 2. In-place Insertion Sort on temporary buffer calculates exact P95 without dynamic memory allocation.
 * 3. P95 latency reflects worst-case tail performance, which is vital for real-time SLA verification.
 */
static inline void ipc_add_sample(IpcStats *s, uint32_t lat) { /* Add latency measurement to rolling statistics */
    s->samples[s->head] = lat;                       /* Store new latency measurement into circular buffer */
    s->head = (s->head + 1) % IPC_HISTORY_SIZE;      /* Advance head index with circular wraparound */
    if (s->count < IPC_HISTORY_SIZE) {               /* Check if sample array is not yet fully filled */
        s->count++;                                  /* Increment total valid sample count */
    }                                                /* End count check */
    
    uint32_t mn = 0xFFFFFFFFU;                       /* Initialize minimum tracking variable to maximum */
    uint32_t mx = 0;                                 /* Initialize maximum tracking variable to zero */
    uint64_t sum = 0;                                /* Initialize cumulative sum accumulator */
    uint32_t tmp[IPC_HISTORY_SIZE];                  /* Temporary array used for sorting percentiles */
    
    /* Why: Copy valid samples into tmp array to sort without disturbing circular arrival order */
    memcpy(tmp, s->samples, sizeof(uint32_t) * s->count); /* Copy valid samples into temporary array */
    for (uint32_t i = 0; i < s->count; i++) {        /* Iterate over each recorded sample */
        if (tmp[i] < mn) mn = tmp[i];                /* Update lowest recorded latency value */
        if (tmp[i] > mx) mx = tmp[i];                /* Update highest recorded latency value */
        sum += tmp[i];                               /* Accumulate sum for average computation */
    }                                                /* End aggregation loop */
    
    /* Why: Insertion sort is optimal (O(N) best case, low overhead) for small array sizes (N=50) */
    for (uint32_t i = 1; i < s->count; i++) {        /* Insertion sort loop to compute 95th percentile */
        uint32_t key = tmp[i];                       /* Save current element to be inserted */
        int j = (int)i - 1;                          /* Set initial predecessor index */
        while (j >= 0 && tmp[j] > key) {             /* Shift elements greater than key to the right */
            tmp[j + 1] = tmp[j];                     /* Move item one position ahead */
            j--;                                     /* Decrement index */
        }                                            /* End inner shift loop */
        tmp[j + 1] = key;                            /* Place key into its sorted slot */
    }                                                /* End sorting loop */
    
    s->min_us = (s->count > 0) ? mn : 0;             /* Set computed minimum or zero if empty */
    s->max_us = mx;                                  /* Set computed maximum */
    s->avg_us = (s->count > 0) ? (uint32_t)(sum / s->count) : 0; /* Calculate and assign average latency */
    /* Why: Index (count * 0.95) yields the 95th percentile worst-case latency value */
    s->p95_us = (s->count > 0) ? tmp[(uint32_t)(s->count * 0.95f)] : 0; /* Extract 95th percentile */
}                                                    /* Finish statistics calculation */

#endif /* CLUSTER_PROTOCOL_H */                     /* End of header file guard */
