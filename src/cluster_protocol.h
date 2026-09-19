/*
 * cluster_protocol.h - Common definitions, structures, and utilities.
 * Provides portable POSIX abstractions and data types for all smart city nodes.
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

#define PLATFORM_NAME          "Smart City RTOS Monitor"  /* Application platform title string */
#define TELEMETRY_PORT         5555                       /* Default TCP port for telemetry transfer */
#define TELEMETRY_INTERVAL_MS  500                        /* Telemetry publishing period in milliseconds */
#define MONITOR_INTERVAL_MS    100                        /* Local system monitoring period in milliseconds */
#define MAX_TASKS              3                          /* Number of simulated city service tasks */
#define TRACE_BUFFER_SIZE      64                         /* Maximum event entries in the circular trace log */
#define IPC_HISTORY_SIZE       50                         /* Number of latency samples kept for statistics */
#define MAX_ACTIVE_FAULTS      16                         /* Maximum number of concurrent faults recorded */

#define PRIORITY_FAULT_DETECTOR 25  /* Priority level for fault detector thread */
#define PRIORITY_MONITOR        20  /* Priority level for local monitor thread */
#define PRIORITY_SERVICE_A      15  /* Priority level for Service A (Traffic Signal) */
#define PRIORITY_SERVICE_B      14  /* Priority level for Service B (Power Grid) */
#define PRIORITY_SERVICE_C      13  /* Priority level for Service C (Environment Sensor) */
#define PRIORITY_TELEMETRY      10  /* Priority level for network telemetry thread */
#define PRIORITY_GPIO            8  /* Priority level for simulated GPIO polling thread */
#define PRIORITY_CLI             5  /* Priority level for diagnostic command line interface */

#define CPU_WARN_THRESHOLD      70.0f    /* Warning threshold for total CPU usage percentage */
#define CPU_CRIT_THRESHOLD      85.0f    /* Critical threshold for total CPU usage percentage */
#define IPC_WARN_LATENCY_US     220000U  /* Warning threshold for IPC message latency in microseconds */
#define IPC_CRIT_LATENCY_US     300000U  /* Critical threshold for IPC message latency in microseconds */

typedef enum {               /* Enumeration of possible states for a workload task */
    TASK_STATE_RUNNING = 0,  /* Task is executing normally within deadlines */
    TASK_STATE_WARNING,      /* Task is experiencing delayed execution or slow heartbeats */
    TASK_STATE_STARVED,      /* Task has stopped receiving execution cycles or timed out */
    TASK_STATE_STOPPED       /* Task is halted */
} TaskState;                 /* Type name for task state enumeration */

typedef enum {                    /* Enumeration of fault classifications */
    FAULT_NONE = 0,               /* No fault detected */
    FAULT_TASK_STARVATION,        /* Workload task missed expected periodic heartbeat */
    FAULT_DEADLINE_MISS,          /* Task execution duration exceeded its deadline limit */
    FAULT_CPU_OVERLOAD,           /* Overall processor usage exceeded safe threshold */
    FAULT_IPC_TIMEOUT,            /* Inter-task communication exceeded round-trip latency limit */
    FAULT_NODE_DISCONNECTED       /* Network connectivity to remote node was interrupted */
} FaultType;                      /* Type name for fault classification enum */

typedef enum {         /* Enumeration of severity levels for reported faults */
    SEV_INFO = 0,      /* Informational message with no operational impact */
    SEV_WARNING,       /* Warning condition requiring attention */
    SEV_CRITICAL       /* Critical failure requiring immediate intervention */
} FaultSeverity;       /* Type name for fault severity enum */

typedef enum {     /* Enumeration representing the health status LED colors */
    LED_GREEN = 0, /* System operates normally with no warnings or errors */
    LED_YELLOW,    /* System encountered one or more warning conditions */
    LED_RED        /* System encountered one or more critical errors */
} HealthLedState;  /* Type name for LED state enumeration */

typedef enum {            /* Enumeration of traced task runtime events */
    EVENT_TASK_START = 1, /* Triggered when a workload task begins an execution cycle */
    EVENT_TASK_END,       /* Triggered when a workload task completes an execution cycle */
    EVENT_HEARTBEAT,      /* Triggered when a task emits a periodic liveness pulse */
    EVENT_FAULT,          /* Triggered when a fault condition is detected and logged */
    EVENT_IPC_XFER        /* Triggered upon completing an inter-process data exchange */
} TraceEventType;         /* Type name for trace event type enumeration */

typedef enum {             /* Remote control command types sent from Supervisor to Node 1 */
    CMD_NONE = 0,          /* No command pending (idle acknowledgment) */
    CMD_CLEAR_ALL,         /* Clear all active faults and simulated injection states */
    CMD_INJECT_STARVE,     /* Inject starvation simulation into a target task */
    CMD_INJECT_DEADLINE,   /* Inject processing delay causing deadline misses */
    CMD_INJECT_CPU,        /* Turn synthetic CPU overload simulation on or off */
    CMD_INJECT_IPC         /* Inject artificial latency delay into IPC exchanges */
} ControlCmdType;          /* Type name for remote control command enum */

typedef struct {               /* Control block structure storing telemetry for an individual task */
    uint32_t  task_id;         /* Numeric identifier for the task (1, 2, or 3) */
    char      name[32];        /* Human-readable description of the task function */
    uint32_t  period_ms;       /* Desired recurrence interval of the task in milliseconds */
    uint32_t  deadline_ms;     /* Maximum allowable execution duration in milliseconds */
    uint32_t  priority;        /* Scheduled priority assigned to this task thread */
    uint32_t  heartbeat;       /* Monotonically increasing counter of task heartbeat pulses */
    uint64_t  last_heartbeat_time_us; /* Timestamp of the most recent heartbeat in microseconds */
    uint32_t  last_exec_us;    /* Duration of the task's most recent execution in microseconds */
    uint32_t  max_exec_us;     /* Peak execution duration recorded across all runs */
    uint32_t  total_cycles;    /* Cumulative number of execution cycles completed */
    uint32_t  deadline_misses; /* Count of cycles where execution exceeded deadline_ms */
    float     cpu_usage_pct;   /* Estimated percentage of total CPU consumed by this task */
    TaskState state;           /* Current operational status (Running, Warning, Starved) */
    bool      inject_starvation;   /* Flag to simulate thread starvation by suppressing heartbeats */
    uint32_t  inject_exec_delay_ms;/* Artificial busy-wait delay added to simulate overload */
} TaskControlBlock;            /* Type name for task control block */

typedef struct {               /* Record describing a detected anomaly or system fault */
    uint32_t      fault_id;     /* Unique sequential identification number for this fault */
    uint32_t      node_id;      /* Originating node identifier (1 for Node1, 2 for Supervisor) */
    uint32_t      task_id;      /* Associated task ID or 0 if system-wide */
    FaultType     type;         /* Category classification of the fault */
    FaultSeverity severity;     /* Severity rank (Warning vs Critical) */
    uint64_t      timestamp_us; /* Timestamp at which the fault was logged */
    char          description[64]; /* Descriptive explanation string of the fault cause */
    bool          active;       /* Boolean flag indicating if fault condition is ongoing */
} FaultRecord;                 /* Type name for fault record structure */

typedef struct {                         /* Historical statistics tracker for IPC transfer latencies */
    uint32_t samples[IPC_HISTORY_SIZE];  /* Rolling circular buffer of recent latency measurements */
    uint32_t count;                      /* Number of valid entries populated in the samples array */
    uint32_t head;                       /* Index position for the next arriving sample */
    uint32_t min_us;                     /* Minimum latency recorded in the active sample window */
    uint32_t max_us;                     /* Maximum latency recorded in the active sample window */
    uint32_t avg_us;                     /* Mathematical mean latency in microseconds */
    uint32_t p95_us;                     /* 95th percentile worst-case latency in microseconds */
} IpcStats;                              /* Type name for IPC statistics structure */

typedef struct {               /* Entry in the lightweight diagnostic trace log */
    uint64_t       timestamp_us; /* Monotonic clock timestamp when the event occurred */
    uint32_t       task_id;      /* Identifier of the task associated with the event */
    TraceEventType event_type;   /* Category of event (Start, End, Heartbeat, Fault, IPC) */
    uint32_t       duration_us;  /* Duration metric associated with the event if applicable */
} TraceRecord;                 /* Type name for trace event record */

typedef struct {                             /* Ring buffer storing recent operational trace records */
    TraceRecord records[TRACE_BUFFER_SIZE];  /* Fixed-size circular array of trace records */
    uint32_t    head;                        /* Current insertion cursor index */
    uint32_t    count;                       /* Total count of valid records currently stored */
} TraceRingBuffer;                           /* Type name for trace ring buffer */

#define SC_MSG_TELEMETRY   1  /* Message header identifier for periodic telemetry packets */
#define SC_MSG_HEARTBEAT   2  /* Message header identifier for task heartbeat signals */
#define SC_MSG_IPC_TRAFFIC 3  /* Message header identifier for inter-service communication */

typedef struct {            /* Command packet payload sent from Supervisor to Node 1 */
    ControlCmdType type;    /* Action requested by supervisor (clear, inject, etc.) */
    uint32_t       task_id; /* Target task index (1-3) or 0 if global */
    uint32_t       value;   /* Integer parameter (delay in ms, or boolean flag 1/0) */
} ControlCmd;               /* Type name for supervisor remote control command */

typedef struct {                                 /* Comprehensive telemetry packet sent from Node 1 */
    uint16_t         type;                       /* Packet type identifier (SC_MSG_TELEMETRY) */
    uint32_t         node_id;                    /* Source node identifier (value 1) */
    uint64_t         timestamp_us;               /* Microsecond timestamp of transmission */
    float            cpu;                        /* Current aggregate CPU utilization percentage */
    int32_t          led;                        /* Current health status LED color state */
    IpcStats         ipc;                        /* IPC latency profile statistics */
    TaskControlBlock tasks[MAX_TASKS];           /* Current operational snapshot for all 3 tasks */
    FaultRecord      fault_map[MAX_ACTIVE_FAULTS]; /* Active and recent fault registry */
    uint32_t         fault_counter;              /* Lifetime total fault count observed */
    TraceRingBuffer  trace_buf;                  /* Recent event trace history for remote viewing */
    bool             inject_cpu_overload;        /* Current state of the CPU burner simulation */
    uint32_t         ipc_inject_delay_ms;        /* Current injected IPC delay in milliseconds */
} TelemetryMsg;                                  /* Type name for telemetry message structure */

typedef struct {          /* Acknowledgment packet returned by Supervisor to Node 1 */
    uint16_t   status;    /* Acknowledgment status code (EOK on success) */
    ControlCmd cmd;       /* Piggybacked remote control command for Node 1 to execute */
} TelemetryAck;           /* Type name for telemetry acknowledgment structure */

typedef struct {            /* Internal task heartbeat ping message */
    uint16_t type;          /* Message type (SC_MSG_HEARTBEAT) */
    uint32_t task_id;       /* ID of the reporting task */
    uint64_t timestamp_us;  /* Monotonic timestamp of the heartbeat event */
} HeartbeatMsg;             /* Type name for heartbeat message */

typedef struct {            /* Reply packet for heartbeat signals */
    uint16_t status;        /* Response status code */
} HeartbeatReply;           /* Type name for heartbeat reply */

typedef struct {            /* Inter-service data transmission message */
    uint16_t type;          /* Message type identifier (SC_MSG_IPC_TRAFFIC) */
    uint32_t sequence;      /* Incremental sequence counter */
    uint64_t send_time_us;  /* Monotonic timestamp when packet was dispatched */
    char     payload[64];   /* Character payload containing sensor or control data */
} IpcTrafficMsg;            /* Type name for IPC traffic message */

typedef struct {            /* Response packet confirming IPC data reception */
    uint16_t status;        /* Response status code */
    uint64_t recv_time_us;  /* Monotonic timestamp when receiver received the message */
} IpcTrafficReply;          /* Type name for IPC traffic reply */

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

static inline uint64_t get_time_us(void) {           /* Retrieve monotonic timestamp in microseconds */
    struct timespec ts;                              /* Allocate timespec structure on the stack */
    clock_gettime(CLOCK_MONOTONIC, &ts);             /* Fetch current time using monotonic clock */
    return ((uint64_t)ts.tv_sec * 1000000ULL) +      /* Multiply whole seconds by 1,000,000 to get us */
           ((uint64_t)ts.tv_nsec / 1000ULL);         /* Divide fractional nanoseconds by 1,000 to get us */
}                                                    /* Return calculated timestamp */

static inline uint64_t get_clock_cycles(void) {      /* Monotonic time representation for cycle counters */
    struct timespec ts;                              /* Allocate timespec structure on the stack */
    clock_gettime(CLOCK_MONOTONIC, &ts);             /* Read monotonic system clock */
    return ((uint64_t)ts.tv_sec * 1000000000ULL) +   /* Multiply seconds to obtain nanosecond count */
           (uint64_t)ts.tv_nsec;                     /* Add residual nanoseconds to total */
}                                                    /* Return total monotonic nanoseconds */

static inline void sleep_ms(uint32_t ms) {           /* Sleep execution for specified milliseconds */
    struct timespec req;                             /* Allocate request timespec structure */
    req.tv_sec  = (time_t)(ms / 1000);               /* Extract full seconds portion */
    req.tv_nsec = (long)(ms % 1000) * 1000000L;      /* Convert remaining milliseconds to nanoseconds */
    nanosleep(&req, NULL);                           /* Suspend thread execution via nanosleep */
}                                                    /* Finish sleep function */

static inline int rt_thread_create(rt_thread_t *t, int pri, void *(*fn)(void *), void *arg) { /* Create thread */
    (void)pri;                                       /* Ignore priority parameter on generic POSIX targets */
    return pthread_create(t, NULL, fn, arg);         /* Launch new POSIX thread with default attributes */
}                                                    /* Return thread creation return code */

static inline void rt_thread_join(rt_thread_t t) {   /* Wait for specified thread to terminate */
    pthread_join(t, NULL);                           /* Block until target thread exits */
}                                                    /* Return from thread join */

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
    memcpy(tmp, s->samples, sizeof(uint32_t) * s->count); /* Copy valid samples into temporary array */
    for (uint32_t i = 0; i < s->count; i++) {        /* Iterate over each recorded sample */
        if (tmp[i] < mn) mn = tmp[i];                /* Update lowest recorded latency value */
        if (tmp[i] > mx) mx = tmp[i];                /* Update highest recorded latency value */
        sum += tmp[i];                               /* Accumulate sum for average computation */
    }                                                /* End aggregation loop */
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
    s->p95_us = (s->count > 0) ? tmp[(uint32_t)(s->count * 0.95f)] : 0; /* Extract 95th percentile */
}                                                    /* Finish statistics calculation */

#endif /* CLUSTER_PROTOCOL_H */                     /* End of header file guard */
