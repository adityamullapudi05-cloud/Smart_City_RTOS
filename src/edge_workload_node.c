/*
 * edge_workload_node.c - Smart City Workload Edge Node (Pi 1).
 * Executes three city services (Traffic, Grid, Environment), local monitoring,
 * and exchanges telemetry and remote control commands with the Supervisor over TCP.
 * All interactive CLI controls are managed remotely from the Supervisor.
 */

#include "cluster_protocol.h"  /* Include shared structures, definitions, and utilities */

extern int   hal_gpio_init(void);                     /* Declare external GPIO initialization routine */
extern void  hal_gpio_set_led(HealthLedState state);  /* Declare external routine to update LED indicator */
extern void *hal_gpio_poll_thread(void *arg);         /* Declare external GPIO input polling thread */
extern void  hal_gpio_deinit(void);                   /* Declare external GPIO cleanup routine */

typedef struct {                                      /* Node 1 global runtime context structure */
    volatile bool    running;                         /* Global execution flag controlling all thread loops */
    float            total_cpu_pct;                   /* Current aggregate CPU utilization percentage */
    TaskControlBlock tasks[MAX_TASKS];                /* Task control blocks for Services A, B, and C */
    rt_mutex_t       task_mutex;                      /* Mutex protecting shared access to task control blocks */
    IpcStats         ipc_stats;                       /* Rolling latency statistics for inter-service IPC */
    uint32_t         ipc_inject_delay_ms;             /* Injected artificial latency for IPC operations */
    rt_mutex_t       ipc_mutex;                       /* Mutex guarding IPC operations and statistics */
    FaultRecord      fault_map[MAX_ACTIVE_FAULTS];    /* Registry table of active and historic fault conditions */
    uint32_t         fault_counter;                   /* Monotonically increasing unique fault counter */
    rt_mutex_t       fault_mutex;                     /* Mutex protecting updates to the fault registry */
    HealthLedState   led_state;                       /* Current active health LED color state */
    TraceRingBuffer  trace_buf;                       /* Circular log buffer recording recent runtime events */
    rt_mutex_t       trace_mutex;                     /* Mutex protecting updates to the trace ring buffer */
    bool             inject_cpu_overload;             /* Flag enabling synthetic CPU burner workload */
    char             supervisor_ip[64];               /* Destination IP address of the Supervisor node */
    int              supervisor_port;                 /* Destination TCP port for Supervisor telemetry */
    bool             tcp_connected;                   /* Status flag indicating active TCP connection */
} Node1Context;                                       /* End of Node1Context definition */

static Node1Context g;                                /* Singleton global context instance for Node 1 */

static void hal_set_led(HealthLedState state) {       /* Update internal LED state and hardware simulation */
    if (g.led_state == state) return;                 /* Avoid redundant calls if state hasn't changed */
    g.led_state = state;                              /* Save requested LED state in global context */
    hal_gpio_set_led(state);                          /* Pass new state to the GPIO HAL layer */
}                                                     /* Return from LED setter function */

static void trace_record(uint32_t task_id, TraceEventType type, uint32_t dur_us) { /* Log an event to trace buffer */
    RT_MUTEX_LOCK(&g.trace_mutex);                    /* Acquire exclusive lock on trace ring buffer */
    uint32_t idx = g.trace_buf.head;                  /* Read current head position in circular buffer */
    g.trace_buf.records[idx].timestamp_us = get_time_us(); /* Stamp current monotonic microsecond time */
    g.trace_buf.records[idx].task_id      = task_id;  /* Store associated task identifier */
    g.trace_buf.records[idx].event_type   = type;     /* Store category of runtime event */
    g.trace_buf.records[idx].duration_us  = dur_us;   /* Store duration metric if applicable */
    g.trace_buf.head = (idx + 1) % TRACE_BUFFER_SIZE; /* Advance circular insertion index */
    if (g.trace_buf.count < TRACE_BUFFER_SIZE) {      /* Check if buffer capacity has not yet maxed out */
        g.trace_buf.count++;                          /* Increment total count of recorded trace events */
    }                                                 /* End capacity check */
    RT_MUTEX_UNLOCK(&g.trace_mutex);                  /* Release exclusive lock on trace buffer */
}                                                     /* Return from trace logging function */

static void fault_register(uint32_t task_id, FaultType type, FaultSeverity sev, const char *desc) { /* Register fault */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire exclusive lock on fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Search for existing matching active fault */
        if (g.fault_map[i].active &&                  /* Check if slot contains an active fault */
            g.fault_map[i].task_id == task_id &&      /* Check if task ID matches target */
            g.fault_map[i].type == type) {            /* Check if fault category matches target */
            g.fault_map[i].timestamp_us = get_time_us(); /* Refresh timestamp on recurring fault */
            RT_MUTEX_UNLOCK(&g.fault_mutex);          /* Release lock before early return */
            return;                                   /* Prevent duplicate active entries */
        }                                             /* End matching condition */
    }                                                 /* End search loop */
    int slot = -1;                                    /* Initialize index for locating free slot */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Scan table to find first inactive entry */
        if (!g.fault_map[i].active) {                 /* Check if slot is currently available */
            slot = i;                                 /* Record index of available slot */
            break;                                    /* Cease scanning upon finding first empty slot */
        }                                             /* End slot check */
    }                                                 /* End scan loop */
    if (slot != -1) {                                 /* If a valid free slot was located */
        g.fault_map[slot].fault_id     = ++g.fault_counter; /* Allocate new sequential fault ID */
        g.fault_map[slot].node_id      = 1;           /* Set origin node ID to 1 (Node 1) */
        g.fault_map[slot].task_id      = task_id;     /* Set associated task ID */
        g.fault_map[slot].type         = type;        /* Assign fault category */
        g.fault_map[slot].severity     = sev;         /* Assign severity rating */
        g.fault_map[slot].timestamp_us = get_time_us(); /* Record detection timestamp */
        g.fault_map[slot].active       = true;        /* Flag entry as actively asserted */
        strncpy(g.fault_map[slot].description, desc, sizeof(g.fault_map[slot].description) - 1); /* Copy description */
        trace_record(task_id, EVENT_FAULT, 0);        /* Record fault event in diagnostic trace log */
        if (sev == SEV_CRITICAL) {                    /* If severity is critical */
            hal_set_led(LED_RED);                     /* Immediately switch LED to RED */
        } else if (sev == SEV_WARNING && g.led_state != LED_RED) { /* If warning and not already RED */
            hal_set_led(LED_YELLOW);                  /* Switch LED indicator to YELLOW */
        }                                             /* End LED check */
    }                                                 /* End slot check */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release exclusive lock on fault registry */
}                                                     /* Return from fault registration function */

static void fault_clear(uint32_t task_id, FaultType type) { /* Clear a specific active fault */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire exclusive lock on fault registry */
    bool has_crit = false;                            /* Flag to track remaining critical faults */
    bool has_warn = false;                            /* Flag to track remaining warning faults */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Iterate through entire fault table */
        if (!g.fault_map[i].active) continue;         /* Skip empty or inactive slots */
        if (g.fault_map[i].task_id == task_id &&      /* Check if entry matches target task */
            g.fault_map[i].type == type) {            /* Check if entry matches target type */
            g.fault_map[i].active = false;            /* Deactivate the target fault */
        } else {                                      /* For all other continuing faults */
            if (g.fault_map[i].severity == SEV_CRITICAL) has_crit = true; /* Note persisting critical fault */
            if (g.fault_map[i].severity == SEV_WARNING)  has_warn = true; /* Note persisting warning fault */
        }                                             /* End match check */
    }                                                 /* End loop through fault table */
    if (has_crit) {                                   /* If any critical faults remain active */
        hal_set_led(LED_RED);                         /* Maintain RED LED indication */
    } else if (has_warn) {                            /* Else if any warning faults remain active */
        hal_set_led(LED_YELLOW);                      /* Maintain YELLOW LED indication */
    } else {                                          /* If all faults are resolved */
        hal_set_led(LED_GREEN);                       /* Restore healthy GREEN LED indication */
    }                                                 /* End LED update */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release exclusive lock on fault registry */
}                                                     /* Return from fault clearing function */

static void fault_clear_all(void) {                   /* Reset and clear all faults and injections */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire exclusive lock on fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Loop through each slot in the table */
        g.fault_map[i].active = false;                /* Mark fault entry as inactive */
    }                                                 /* End loop */
    hal_set_led(LED_GREEN);                           /* Restore GREEN LED indicator */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release exclusive lock on fault registry */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire lock to reset task injection flags */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Loop across all tasks */
        g.tasks[i].inject_starvation    = false;      /* Disable simulated starvation */
        g.tasks[i].inject_exec_delay_ms = 0;          /* Clear injected execution delay */
        g.tasks[i].state = TASK_STATE_RUNNING;        /* Restore healthy running state */
    }                                                 /* End task loop */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    g.inject_cpu_overload  = false;                   /* Stop synthetic CPU load generator */
    g.ipc_inject_delay_ms  = 0;                       /* Clear injected IPC latency */
}                                                     /* Return from fault_clear_all */

static void ipc_record_latency(uint32_t lat_us) {     /* Record and analyze IPC round-trip latency */
    RT_MUTEX_LOCK(&g.ipc_mutex);                      /* Acquire lock protecting IPC statistics */
    ipc_add_sample(&g.ipc_stats, lat_us);             /* Insert sample and update rolling metrics */
    RT_MUTEX_UNLOCK(&g.ipc_mutex);                    /* Release lock after stats update */
    if (lat_us >= IPC_CRIT_LATENCY_US) {              /* Check if latency exceeded critical threshold */
        fault_register(2, FAULT_IPC_TIMEOUT, SEV_CRITICAL, "IPC Latency Critical"); /* Log critical fault */
    } else if (lat_us >= IPC_WARN_LATENCY_US) {       /* Check if latency exceeded warning threshold */
        fault_register(2, FAULT_IPC_TIMEOUT, SEV_WARNING,  "IPC Latency Elevated"); /* Log warning fault */
    } else {                                          /* Latency is within acceptable bounds */
        fault_clear(2, FAULT_IPC_TIMEOUT);            /* Clear any existing IPC latency fault */
    }                                                 /* End threshold evaluation */
}                                                     /* Return from latency tracking function */

static void send_heartbeat(TaskControlBlock *t) {     /* Emit heartbeat ping for specified task */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire lock before updating heartbeat counters */
    t->heartbeat++;                                   /* Increment task heartbeat counter */
    t->last_heartbeat_time_us = get_time_us();        /* Record microsecond timestamp of heartbeat */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    trace_record(t->task_id, EVENT_HEARTBEAT, 0);     /* Record heartbeat event in trace log */
}                                                     /* Return from heartbeat dispatch function */

static void task_finish(TaskControlBlock *t, uint64_t t_start, const char *dl_msg) { /* Finalize cycle */
    uint32_t exec = (uint32_t)(get_time_us() - t_start); /* Calculate cycle execution duration in microseconds */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire lock to update task metrics */
    t->last_exec_us = exec;                           /* Save execution duration of latest cycle */
    t->total_cycles++;                                /* Increment cumulative cycle count */
    if (exec > t->max_exec_us) t->max_exec_us = exec;  /* Track worst-case peak execution time */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    trace_record(t->task_id, EVENT_TASK_END, exec);   /* Record task finish event in trace log */
    if (!t->inject_starvation) {                      /* If task starvation is not injected */
        send_heartbeat(t);                            /* Emit regular heartbeat signal */
    }                                                 /* End starvation check */
    if (exec > t->deadline_ms * 1000U) {              /* Check if duration exceeded execution deadline */
        RT_MUTEX_LOCK(&g.task_mutex);                 /* Acquire lock to increment deadline misses */
        t->deadline_misses++;                         /* Increment missed deadline counter */
        RT_MUTEX_UNLOCK(&g.task_mutex);               /* Release task lock */
        fault_register(t->task_id, FAULT_DEADLINE_MISS, SEV_WARNING, dl_msg); /* Register deadline fault */
    } else {                                          /* Execution completed within allotted deadline */
        fault_clear(t->task_id, FAULT_DEADLINE_MISS); /* Clear deadline fault if previously present */
    }                                                 /* End deadline check */
}                                                     /* Return from task finish handler */

static void *service_a_thread(void *arg) {            /* Service A worker thread (Traffic Signal Control) */
    (void)arg;                                        /* Suppress unused parameter warning */
    TaskControlBlock *t = &g.tasks[0];                /* Pointer to Service A control block */
    uint32_t seq = 0;                                 /* Monotonic message sequence number */
    while (g.running) {                               /* Run continuously while system flag is true */
        uint64_t t_start = get_time_us();             /* Capture start time of execution cycle */
        trace_record(t->task_id, EVENT_TASK_START, 0);/* Log start event into trace log */
        sleep_ms(15 + t->inject_exec_delay_ms);       /* Simulate work plus any injected delay */
        uint64_t snd0 = get_time_us();                /* Record timestamp prior to dispatching IPC */
        seq++;                                        /* Increment message sequence counter */
        if (g.ipc_inject_delay_ms > 0) {              /* Check if synthetic IPC latency is injected */
            sleep_ms(g.ipc_inject_delay_ms);          /* Sleep for the injected IPC delay */
        }                                             /* End IPC delay check */
        uint32_t lat = (uint32_t)(get_time_us() - snd0); /* Compute elapsed IPC round-trip latency */
        ipc_record_latency(lat);                      /* Record latency in rolling statistics */
        trace_record(t->task_id, EVENT_IPC_XFER, lat);/* Log IPC transfer event with latency metric */
        task_finish(t, t_start, "Service A exceeded 80 ms deadline"); /* Validate deadline and emit HB */
        sleep_ms(t->period_ms > 20 ? t->period_ms - 20 : 10); /* Wait for next scheduled periodic interval */
    }                                                 /* End of service loop */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from Service A thread */

static void *service_b_thread(void *arg) {            /* Service B worker thread (Power Grid Monitoring) */
    (void)arg;                                        /* Suppress unused parameter warning */
    TaskControlBlock *t = &g.tasks[1];                /* Pointer to Service B control block */
    while (g.running) {                               /* Run loop while system remains active */
        uint64_t t_start = get_time_us();             /* Record timestamp at beginning of cycle */
        trace_record(t->task_id, EVENT_TASK_START, 0);/* Record task start in trace log */
        sleep_ms(25 + t->inject_exec_delay_ms);       /* Simulate power grid workload plus delay */
        task_finish(t, t_start, "Service B exceeded 150 ms deadline"); /* Check deadline and emit HB */
        sleep_ms(t->period_ms > 30 ? t->period_ms - 30 : 10); /* Sleep until next periodic cycle */
    }                                                 /* End of service loop */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from Service B thread */

static void *service_c_thread(void *arg) {            /* Service C worker thread (Environmental Sensing) */
    (void)arg;                                        /* Suppress unused parameter warning */
    TaskControlBlock *t = &g.tasks[2];                /* Pointer to Service C control block */
    while (g.running) {                               /* Loop execution while active */
        uint64_t t_start = get_time_us();             /* Record start timestamp of cycle */
        trace_record(t->task_id, EVENT_TASK_START, 0);/* Log start event into trace buffer */
        sleep_ms(40 + t->inject_exec_delay_ms);       /* Simulate sensor polling workload plus delay */
        task_finish(t, t_start, "Service C exceeded 400 ms deadline"); /* Check deadline and emit HB */
        sleep_ms(t->period_ms > 50 ? t->period_ms - 50 : 20); /* Sleep until next sampling period */
    }                                                 /* End of service loop */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from Service C thread */

static void *cpu_burner_thread(void *arg) {           /* Background worker to simulate CPU overload */
    (void)arg;                                        /* Suppress unused parameter warning */
    volatile double d = 0.0;                          /* Volatile accumulator to prevent compiler optimization */
    while (g.running) {                               /* Continuous execution loop */
        if (g.inject_cpu_overload) {                  /* Check if CPU overload simulation is requested */
            uint64_t s = get_time_us();               /* Record starting timestamp of busy-wait burn */
            while (get_time_us() - s < 80000ULL) {    /* Burn CPU cycles intensely for 80 milliseconds */
                d += sin(d + 1.2345);                 /* Perform trigonometric calculation repeatedly */
            }                                         /* End busy loop */
            sleep_ms(20);                             /* Relinquish CPU briefly for 20 milliseconds */
        } else {                                      /* In normal unasserted operation */
            sleep_ms(200);                            /* Idle sleep while overload is inactive */
        }                                             /* End injection check */
    }                                                 /* End thread loop */
    return NULL;                                      /* Exit thread */
}                                                     /* Return from CPU burner thread */

static void *monitoring_task_thread(void *arg) {      /* Local health and starvation watchdog thread */
    (void)arg;                                        /* Suppress unused parameter warning */
    uint64_t prev_time = get_time_us();               /* Record initial timestamp of previous window */
    while (g.running) {                               /* Run monitor loop continuously */
        sleep_ms(MONITOR_INTERVAL_MS);                /* Wait for monitor sampling period (100 ms) */
        uint64_t now = get_time_us();                 /* Capture current monotonic timestamp */
        uint64_t window_us = now - prev_time;         /* Calculate elapsed duration of monitoring window */
        prev_time = now;                              /* Advance previous timestamp to current time */
        float work_us = 0.0f;                         /* Accumulator for calculated active work time */
        for (int i = 0; i < MAX_TASKS; i++) {         /* Iterate through all registered tasks */
            TaskControlBlock *t = &g.tasks[i];        /* Obtain pointer to task control block */
            uint64_t age_ms = (now - t->last_heartbeat_time_us) / 1000; /* Calculate heartbeat age in ms */
            if (age_ms >= t->period_ms * 3) {         /* Check if heartbeat missed 3 consecutive periods */
                char desc[64];                        /* Buffer to construct descriptive fault string */
                t->state = TASK_STATE_STARVED;        /* Transition task state to Starved */
                snprintf(desc, sizeof(desc), "Task '%s' STARVED", t->name); /* Format fault message */
                fault_register(t->task_id, FAULT_TASK_STARVATION, SEV_CRITICAL, desc); /* Register critical */
            } else if (age_ms >= t->period_ms * 2) {  /* Check if heartbeat missed 2 consecutive periods */
                char desc[64];                        /* Buffer to format warning message */
                t->state = TASK_STATE_WARNING;        /* Transition task state to Warning */
                snprintf(desc, sizeof(desc), "Task '%s' HB slow", t->name); /* Format warning text */
                fault_register(t->task_id, FAULT_TASK_STARVATION, SEV_WARNING, desc); /* Register warning */
            } else if (t->state == TASK_STATE_STARVED || t->state == TASK_STATE_WARNING) { /* If now healthy */
                t->state = TASK_STATE_RUNNING;        /* Restore task state to Running */
                fault_clear(t->task_id, FAULT_TASK_STARVATION); /* Clear task starvation fault */
            }                                         /* End liveness evaluation */
            if (window_us > 0) {                      /* If window duration is valid non-zero */
                float duty = ((float)t->last_exec_us / (float)(t->period_ms * 1000)) * 100.0f; /* Compute duty */
                t->cpu_usage_pct = (duty > 100.0f) ? 100.0f : duty; /* Clamp duty cycle to 100% maximum */
                work_us += t->cpu_usage_pct * 0.01f * (float)window_us; /* Accumulate weighted task busy time */
            }                                         /* End window check */
        }                                             /* End tasks loop */
        float cpu = (window_us > 0) ? (work_us / (float)window_us * 100.0f) : 0.0f; /* Compute CPU percentage */
        if (g.inject_cpu_overload) cpu += 60.0f;      /* Add artificial 60% load if CPU burner is active */
        if (cpu > 100.0f) cpu = 100.0f;               /* Clamp total CPU usage percentage to 100% max */
        g.total_cpu_pct = cpu;                        /* Store computed total CPU in global context */
        if (cpu >= CPU_CRIT_THRESHOLD) {              /* Check if CPU exceeds critical limit */
            fault_register(0, FAULT_CPU_OVERLOAD, SEV_CRITICAL, "CPU Critical"); /* Raise critical fault */
        } else if (cpu >= CPU_WARN_THRESHOLD) {       /* Check if CPU exceeds warning limit */
            fault_register(0, FAULT_CPU_OVERLOAD, SEV_WARNING, "CPU Elevated"); /* Raise warning fault */
        } else {                                      /* CPU is within normal boundaries */
            fault_clear(0, FAULT_CPU_OVERLOAD);       /* Clear CPU overload fault */
        }                                             /* End CPU threshold check */
    }                                                 /* End of monitoring loop */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from monitoring thread */

static void fill_telemetry(TelemetryMsg *m) {         /* Package current state into TelemetryMsg packet */
    memset(m, 0, sizeof(*m));                         /* Zero-initialize the entire packet memory */
    m->type         = SC_MSG_TELEMETRY;               /* Set packet type identifier */
    m->node_id      = 1;                              /* Set node identifier (1 for Node 1) */
    m->timestamp_us = get_time_us();                  /* Stamp current microsecond transmission time */
    m->cpu          = g.total_cpu_pct;                /* Copy aggregate CPU usage percentage */
    m->led          = (int32_t)g.led_state;           /* Copy current health LED state */
    RT_MUTEX_LOCK(&g.ipc_mutex);                      /* Acquire lock to read IPC statistics */
    m->ipc          = g.ipc_stats;                    /* Copy snapshot of IPC statistics */
    RT_MUTEX_UNLOCK(&g.ipc_mutex);                    /* Release IPC lock */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire lock to copy task control blocks */
    for (int i = 0; i < MAX_TASKS; i++) m->tasks[i] = g.tasks[i]; /* Copy all 3 task control blocks */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire lock to copy fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) m->fault_map[i] = g.fault_map[i]; /* Copy fault table */
    m->fault_counter = g.fault_counter;               /* Copy lifetime fault counter */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
    RT_MUTEX_LOCK(&g.trace_mutex);                    /* Acquire lock to copy trace buffer */
    m->trace_buf = g.trace_buf;                       /* Copy ring buffer snapshot */
    RT_MUTEX_UNLOCK(&g.trace_mutex);                  /* Release trace lock */
    m->inject_cpu_overload = g.inject_cpu_overload;   /* Copy current CPU overload injection flag */
    m->ipc_inject_delay_ms = g.ipc_inject_delay_ms;   /* Copy current IPC latency injection delay */
}                                                     /* Return from fill_telemetry */

static void handle_remote_command(const ControlCmd *cmd) { /* Process remote command from Supervisor */
    if (!cmd || cmd->type == CMD_NONE) return;        /* Return immediately if no command received */
    printf("[RemoteCmd] Received command type=%d task=%u val=%u\n", cmd->type, cmd->task_id, cmd->value); /* Log */
    switch (cmd->type) {                              /* Dispatch on command classification */
    case CMD_CLEAR_ALL:                               /* Command to reset all active faults and injections */
        fault_clear_all();                            /* Call clear-all handler */
        break;                                        /* End case */
    case CMD_INJECT_STARVE:                           /* Command to simulate starvation on target task */
        if (cmd->task_id >= 1 && cmd->task_id <= MAX_TASKS) { /* Validate task index range */
            RT_MUTEX_LOCK(&g.task_mutex);             /* Acquire task lock */
            g.tasks[cmd->task_id - 1].inject_starvation = (cmd->value != 0); /* Set starvation flag */
            RT_MUTEX_UNLOCK(&g.task_mutex);           /* Release task lock */
        }                                             /* End validation check */
        break;                                        /* End case */
    case CMD_INJECT_DEADLINE:                         /* Command to inject processing deadline delay */
        if (cmd->task_id >= 1 && cmd->task_id <= MAX_TASKS) { /* Validate task index range */
            RT_MUTEX_LOCK(&g.task_mutex);             /* Acquire task lock */
            g.tasks[cmd->task_id - 1].inject_exec_delay_ms = cmd->value; /* Set execution delay */
            RT_MUTEX_UNLOCK(&g.task_mutex);           /* Release task lock */
        }                                             /* End validation check */
        break;                                        /* End case */
    case CMD_INJECT_CPU:                              /* Command to toggle synthetic CPU burner */
        g.inject_cpu_overload = (cmd->value != 0);    /* Set CPU overload flag based on value */
        break;                                        /* End case */
    case CMD_INJECT_IPC:                              /* Command to set injected IPC latency */
        g.ipc_inject_delay_ms = cmd->value;           /* Update IPC artificial delay in ms */
        break;                                        /* End case */
    default:                                          /* Unknown command type */
        break;                                        /* Do nothing */
    }                                                 /* End switch statement */
}                                                     /* Return from command handler */

static void *telemetry_sender_thread(void *arg) {     /* Dispatches telemetry and receives Supervisor commands */
    (void)arg;                                        /* Suppress unused parameter warning */
    int sock = -1;                                    /* File descriptor for TCP connection */
    TelemetryMsg tm;                                  /* Buffer for outgoing telemetry packet */
    TelemetryAck ack;                                 /* Buffer for incoming Supervisor acknowledgment */
    while (g.running) {                               /* Run continuously while application is active */
        sleep_ms(TELEMETRY_INTERVAL_MS);              /* Wait for telemetry reporting period */
        fill_telemetry(&tm);                          /* Package current node telemetry snapshot */
        if (sock < 0) {                               /* If socket is not currently connected */
            sock = socket(AF_INET, SOCK_STREAM, 0);   /* Create standard TCP socket */
            if (sock < 0) continue;                   /* If socket creation failed, retry next loop */
            struct timeval tv = { 1, 500000 };        /* 1.5 second socket timeout */
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); /* Apply receive timeout */
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)); /* Apply send timeout */
            struct sockaddr_in sv;                    /* Socket address structure for Supervisor */
            memset(&sv, 0, sizeof(sv));               /* Clear structure memory */
            sv.sin_family      = AF_INET;             /* IPv4 address family */
            sv.sin_port        = htons((uint16_t)g.supervisor_port); /* Set supervisor destination port */
            sv.sin_addr.s_addr = inet_addr(g.supervisor_ip);         /* Set supervisor destination IP */
            if (connect(sock, (struct sockaddr *)&sv, sizeof(sv)) < 0) { /* Attempt TCP connection */
                close(sock);                          /* Close failed socket */
                sock = -1;                            /* Reset descriptor to invalid */
                g.tcp_connected = false;              /* Mark status as disconnected */
                continue;                             /* Retry connection in next iteration */
            }                                         /* End connect check */
            printf("[Telemetry] Connected to Supervisor at %s:%d\n", g.supervisor_ip, g.supervisor_port); /* Log */
            g.tcp_connected = true;                   /* Flag active connection */
        }                                             /* End connection logic */
        if (send(sock, &tm, sizeof(tm), 0) != (ssize_t)sizeof(tm) || /* Send telemetry packet */
            recv(sock, &ack, sizeof(ack), MSG_WAITALL) <= 0) {       /* Await acknowledgment from supervisor */
            printf("[Telemetry] Connection to Supervisor lost. Reconnecting...\n"); /* Log disconnect */
            close(sock);                              /* Close dead socket */
            sock = -1;                                /* Reset descriptor */
            g.tcp_connected = false;                  /* Mark disconnected */
            continue;                                 /* Reconnect on next cycle */
        }                                             /* End send/recv check */
        handle_remote_command(&ack.cmd);              /* Execute any remote control command piggybacked in ACK */
    }                                                 /* End thread loop */
    if (sock >= 0) close(sock);                       /* Close open socket upon shutdown */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from telemetry sender thread */

int main(int argc, char *argv[]) {                    /* Application entry point for Node 1 */
    signal(SIGPIPE, SIG_IGN);                         /* Ignore SIGPIPE signals caused by broken sockets */
    memset(&g, 0, sizeof(g));                         /* Zero-initialize global context */
    g.running         = true;                         /* Set active execution flag */
    g.led_state       = LED_GREEN;                    /* Start with healthy GREEN LED status */
    g.supervisor_port = TELEMETRY_PORT;               /* Assign default telemetry TCP port */
    strncpy(g.supervisor_ip, "169.254.178.16", sizeof(g.supervisor_ip) - 1); /* Default supervisor IP */
    bool enable_gpio = true;                          /* Enable simulated GPIO by default */
    for (int i = 1; i < argc; i++) {                  /* Parse command line arguments */
        if (!strncmp(argv[i], "--ip=", 5)) {          /* Check for supervisor IP override */
            strncpy(g.supervisor_ip, argv[i] + 5, sizeof(g.supervisor_ip) - 1); /* Copy custom IP */
        }                                             /* End IP check */
        if (!strncmp(argv[i], "--port=", 7)) {        /* Check for supervisor port override */
            g.supervisor_port = atoi(argv[i] + 7);    /* Convert and store port number */
        }                                             /* End port check */
        if (!strcmp(argv[i], "--no-gpio")) {          /* Check for disabling GPIO flag */
            enable_gpio = false;                      /* Disable GPIO initialization */
        }                                             /* End GPIO check */
    }                                                 /* End argument parsing */
    RT_MUTEX_INIT(&g.task_mutex);                     /* Initialize task control block mutex */
    RT_MUTEX_INIT(&g.ipc_mutex);                      /* Initialize IPC metrics mutex */
    RT_MUTEX_INIT(&g.fault_mutex);                    /* Initialize fault registry mutex */
    RT_MUTEX_INIT(&g.trace_mutex);                    /* Initialize trace ring buffer mutex */
    g.tasks[0] = (TaskControlBlock){ .task_id = 1, .period_ms = 100, .deadline_ms = 80, /* Configure Task 1 */
        .priority = PRIORITY_SERVICE_A, .state = TASK_STATE_RUNNING };                   /* Assign priority */
    strncpy(g.tasks[0].name, "Service_A (Traffic)", 31);                                 /* Assign name */
    g.tasks[0].last_heartbeat_time_us = get_time_us();                                   /* Init heartbeat */
    g.tasks[1] = (TaskControlBlock){ .task_id = 2, .period_ms = 200, .deadline_ms = 150, /* Configure Task 2 */
        .priority = PRIORITY_SERVICE_B, .state = TASK_STATE_RUNNING };                   /* Assign priority */
    strncpy(g.tasks[1].name, "Service_B (Grid)", 31);                                    /* Assign name */
    g.tasks[1].last_heartbeat_time_us = get_time_us();                                   /* Init heartbeat */
    g.tasks[2] = (TaskControlBlock){ .task_id = 3, .period_ms = 500, .deadline_ms = 400, /* Configure Task 3 */
        .priority = PRIORITY_SERVICE_C, .state = TASK_STATE_RUNNING };                   /* Assign priority */
    strncpy(g.tasks[2].name, "Service_C (Env)", 31);                                     /* Assign name */
    g.tasks[2].last_heartbeat_time_us = get_time_us();                                   /* Init heartbeat */
    printf("===================================================================\n");     /* Print banner */
    printf(" %s - NODE 1 (Workload Node)\n", PLATFORM_NAME);                             /* Print title */
    printf(" Supervisor Target: %s:%d | GPIO Simulation: %s\n",                          /* Print config */
           g.supervisor_ip, g.supervisor_port, enable_gpio ? "ON" : "OFF");              /* Print details */
    printf(" Note: Local CLI removed. All diagnostics & controls run from Supervisor.\n");/* Print CLI note */
    printf("===================================================================\n");     /* Print line */
    if (enable_gpio) hal_gpio_init();                 /* Initialize simulated GPIO subsystem if enabled */
    rt_thread_t th_sa, th_sb, th_sc, th_mon, th_burn, th_tcp, th_gpio; /* Thread handle identifiers */
    rt_thread_create(&th_sa, PRIORITY_SERVICE_A, service_a_thread, NULL); /* Start Service A */
    rt_thread_create(&th_sb, PRIORITY_SERVICE_B, service_b_thread, NULL); /* Start Service B */
    rt_thread_create(&th_sc, PRIORITY_SERVICE_C, service_c_thread, NULL); /* Start Service C */
    rt_thread_create(&th_mon, PRIORITY_MONITOR, monitoring_task_thread, NULL); /* Start Monitor thread */
    rt_thread_create(&th_burn, PRIORITY_CLI, cpu_burner_thread, NULL); /* Start CPU Burner thread */
    rt_thread_create(&th_tcp, PRIORITY_TELEMETRY, telemetry_sender_thread, NULL); /* Start Telemetry thread */
    if (enable_gpio) {                                /* Check if GPIO thread should be spawned */
        rt_thread_create(&th_gpio, PRIORITY_GPIO, hal_gpio_poll_thread, NULL); /* Start GPIO poller */
    }                                                 /* End GPIO thread check */
    printf("[Node1] All workload tasks started. Streaming telemetry to Supervisor...\n"); /* Log status */
    rt_thread_join(th_tcp);                           /* Wait on telemetry thread loop */
    g.running = false;                                /* Signal all threads to terminate upon exit */
    sleep_ms(300);                                    /* Allow background threads time to exit cleanly */
    if (enable_gpio) hal_gpio_deinit();               /* Deinitialize GPIO subsystem */
    return 0;                                         /* Return success code */
}                                                     /* End of main function */
