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

typedef struct {                                      /* Rationale: Centralized singleton context consolidating edge tasks, telemetry, and mutexes */
    volatile bool    running;                         /* Why: Marked volatile so compiler prevents register caching; coordinates synchronous termination */
    float            total_cpu_pct;                   /* Why: Aggregate processor utilization computed from real-time task duty cycles and CPU burner */
    TaskControlBlock tasks[MAX_TASKS];                /* Why: Static TCB array for Service A (Traffic), Service B (Grid), and Service C (Env) */
    rt_mutex_t       task_mutex;                      /* Why: Fine-grained mutex guarding task states and heartbeats against concurrent telemetry reads */
    IpcStats         ipc_stats;                       /* Why: Rolling circular buffer tracking inter-service message round-trip latency */
    uint32_t         ipc_inject_delay_ms;             /* Why: Artificial transmission delay injected remotely via supervisor to test SLA timeouts */
    rt_mutex_t       ipc_mutex;                       /* Why: Mutex protecting IPC latency samples from race conditions */
    FaultRecord      fault_map[MAX_ACTIVE_FAULTS];    /* Why: Statically allocated active fault registry (prevents dynamic heap fragmentation) */
    uint32_t         fault_counter;                   /* Why: Monotonic counter for unique fault IDs; benchmarks historical self-healing */
    rt_mutex_t       fault_mutex;                     /* Why: Mutex guarding fault registry between monitor thread and telemetry transmitter */
    HealthLedState   led_state;                       /* Why: Active edge hardware LED status (Green/Yellow/Red) driven by worst active fault */
    TraceRingBuffer  trace_buf;                       /* Why: Chronological event trace ring buffer (Task Start, End, Heartbeats, Faults) */
    rt_mutex_t       trace_mutex;                     /* Why: Mutex synchronizing trace buffer writes from tasks against telemetry packaging */
    bool             inject_cpu_overload;             /* Why: Flag activating synthetic busy-wait CPU burner thread to simulate overload */
    char             supervisor_ip[64];               /* Why: Target IP address of central supervisor control plane node (Pi 2) */
    int              supervisor_port;                 /* Why: Target TCP port (8080) for streaming outbound telemetry */
    bool             tcp_connected;                   /* Why: Connection status flag indicating healthy socket to supervisor */
} Node1Context;                                       /* End of Node1Context definition */

static Node1Context g;                                /* Singleton global context instance for Node 1: zero-initialized */

static void hal_set_led(HealthLedState state) {       /* Rationale: Edge node visual health indicator state manager */
    if (g.led_state == state) return;                 /* Why: Optimization: suppresses redundant hardware register writes and log messages */
    g.led_state = state;                              /* Why: Stores state in global context so telemetry packet reflects true hardware indicator */
    hal_gpio_set_led(state);                          /* Why: Dispatches state to GPIO Hardware Abstraction Layer */
}                                                     /* Return from hal_set_led */

static void trace_record(uint32_t task_id, TraceEventType type, uint32_t dur_us) { /* Rationale: Low-overhead sub-microsecond event trace logger */
    RT_MUTEX_LOCK(&g.trace_mutex);                    /* Thread-safety: Guard circular trace buffer from concurrent task writers */
    uint32_t idx = g.trace_buf.head;                  /* Read current circular insertion index */
    g.trace_buf.records[idx].timestamp_us = get_time_us(); /* Why: Monotonic timestamp captures precise real-time event moment */
    g.trace_buf.records[idx].task_id      = task_id;  /* Task handle (1=Traffic, 2=Grid, 3=Env) */
    g.trace_buf.records[idx].event_type   = type;     /* Event category (START, END, HEARTBEAT, FAULT, IPC_XFER) */
    g.trace_buf.records[idx].duration_us  = dur_us;   /* Elapsed execution or transfer duration in microseconds */
    g.trace_buf.head = (idx + 1) % TRACE_BUFFER_SIZE; /* Why: Advance circular buffer head with modular wraparound */
    if (g.trace_buf.count < TRACE_BUFFER_SIZE) {      /* Saturating counter check */
        g.trace_buf.count++;                          /* Increment total count up to buffer limit */
    }                                                 /* End capacity check */
    RT_MUTEX_UNLOCK(&g.trace_mutex);                  /* Release trace buffer lock */
}                                                     /* Return from trace_record */

static void fault_register(uint32_t task_id, FaultType type, FaultSeverity sev, const char *desc) { /* Rationale: Autonomous edge fault registration */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Acquire exclusive lock on local fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* De-duplication loop: scan for existing active fault */
        if (g.fault_map[i].active &&                  /* Active entry */
            g.fault_map[i].task_id == task_id &&      /* Same task ID */
            g.fault_map[i].type == type) {            /* Same fault category */
            g.fault_map[i].timestamp_us = get_time_us(); /* Why: Refresh timestamp without wasting new table slots */
            RT_MUTEX_UNLOCK(&g.fault_mutex);          /* Release lock */
            return;                                   /* Prevent duplicate active entries */
        }                                             /* End match check */
    }                                                 /* End search loop */
    int slot = -1;                                    /* Slot finder variable */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Search for an available inactive slot in static array */
        if (!g.fault_map[i].active) {                 /* Inactive slot located */
            slot = i;                                 /* Allocate slot */
            break;                                    /* Stop search */
        }                                             /* End check */
    }                                                 /* End scan loop */
    if (slot != -1) {                                 /* Slot allocated */
        g.fault_map[slot].fault_id     = ++g.fault_counter; /* Monotonic unique ID */
        g.fault_map[slot].node_id      = 1;           /* Originating Node ID (1 = Edge Node) */
        g.fault_map[slot].task_id      = task_id;     /* Affected task handle */
        g.fault_map[slot].type         = type;        /* Category (Starvation, Deadline, Overload, IPC) */
        g.fault_map[slot].severity     = sev;         /* Severity (Warning vs Critical) */
        g.fault_map[slot].timestamp_us = get_time_us(); /* Detection timestamp */
        g.fault_map[slot].active       = true;        /* Flag as currently asserted */
        strncpy(g.fault_map[slot].description, desc, sizeof(g.fault_map[slot].description) - 1); /* Diagnostic text */
        trace_record(task_id, EVENT_FAULT, 0);        /* Why: Record fault in event trace log for timeline playback */
        if (sev == SEV_CRITICAL) {                    /* Critical fault escalation */
            hal_set_led(LED_RED);                     /* Why: Critical failures immediately override visual LED to RED */
        } else if (sev == SEV_WARNING && g.led_state != LED_RED) { /* Warning escalation */
            hal_set_led(LED_YELLOW);                  /* Why: Set YELLOW only if not already masked by RED */
        }                                             /* End LED check */
    }                                                 /* End slot check */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault registry lock */
}                                                     /* Return from fault_register */

static void fault_clear(uint32_t task_id, FaultType type) { /* Rationale: Autonomous edge self-healing and LED recovery */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Acquire lock to update fault table */
    bool has_crit = false;                            /* Track persisting critical faults */
    bool has_warn = false;                            /* Track persisting warning faults */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Scan all allocated fault slots */
        if (!g.fault_map[i].active) continue;         /* Skip empty slots */
        if (g.fault_map[i].task_id == task_id &&      /* Target task */
            g.fault_map[i].type == type) {            /* Target fault type */
            g.fault_map[i].active = false;            /* Why: Deactivate resolved fault entry */
        } else {                                      /* Evaluate surviving faults */
            if (g.fault_map[i].severity == SEV_CRITICAL) has_crit = true; /* Critical fault remains */
            if (g.fault_map[i].severity == SEV_WARNING)  has_warn = true; /* Warning fault remains */
        }                                             /* End check */
    }                                                 /* End scan loop */
    if (has_crit) {                                   /* Tier 1: Critical fault survives */
        hal_set_led(LED_RED);                         /* Maintain RED LED indication */
    } else if (has_warn) {                            /* Tier 2: Warning fault survives */
        hal_set_led(LED_YELLOW);                      /* Maintain YELLOW LED indication */
    } else {                                          /* Tier 3: All faults clear */
        hal_set_led(LED_GREEN);                       /* Why: Fully self-healed: restore nominal GREEN LED */
    }                                                 /* End LED update */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
}                                                     /* Return from fault_clear */

static void fault_clear_all(void) {                   /* Rationale: Full edge node recovery and fault injection reset */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire fault lock */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Loop through fault table */
        g.fault_map[i].active = false;                /* Deactivate all local faults */
    }                                                 /* End loop */
    hal_set_led(LED_GREEN);                           /* Restore GREEN LED indicator */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire task lock to clear synthetic delays */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Reset all 3 task control blocks */
        g.tasks[i].inject_starvation    = false;      /* Disable simulated thread starvation */
        g.tasks[i].inject_exec_delay_ms = 0;          /* Clear synthetic execution delay */
        g.tasks[i].state = TASK_STATE_RUNNING;        /* Restore nominal running state */
    }                                                 /* End task loop */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    g.inject_cpu_overload  = false;                   /* Why: Deactivate CPU burner workload */
    g.ipc_inject_delay_ms  = 0;                       /* Why: Reset artificial IPC transmission delay */
}                                                     /* Return from fault_clear_all */

static void ipc_record_latency(uint32_t lat_us) {     /* Rationale: Inter-Task message passing latency evaluator */
    RT_MUTEX_LOCK(&g.ipc_mutex);                      /* Thread-safety: Guard IPC statistics */
    ipc_add_sample(&g.ipc_stats, lat_us);             /* Why: Add sample to circular buffer and recalculate Min, Max, Avg, P95 */
    RT_MUTEX_UNLOCK(&g.ipc_mutex);                    /* Release lock */
    if (lat_us >= IPC_CRIT_LATENCY_US) {              /* SLA Check: Latency >= 300 ms */
        fault_register(2, FAULT_IPC_TIMEOUT, SEV_CRITICAL, "IPC Latency Critical"); /* Critical timeout alarm */
    } else if (lat_us >= IPC_WARN_LATENCY_US) {       /* SLA Check: Latency >= 220 ms */
        fault_register(2, FAULT_IPC_TIMEOUT, SEV_WARNING,  "IPC Latency Elevated"); /* Elevated latency warning */
    } else {                                          /* Latency < 220 ms */
        fault_clear(2, FAULT_IPC_TIMEOUT);            /* Self-healing: clear IPC latency fault */
    }                                                 /* End threshold check */
}                                                     /* Return from ipc_record_latency */

static void send_heartbeat(TaskControlBlock *t) {     /* Rationale: Monotonic task liveness pulse emitter */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire task lock */
    t->heartbeat++;                                   /* Why: Monotonic counter proves task loop is making continuous forward progress */
    t->last_heartbeat_time_us = get_time_us();        /* Why: Timestamp audited by watchdog to detect frozen or deadlocked threads */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    trace_record(t->task_id, EVENT_HEARTBEAT, 0);     /* Record heartbeat pulse in event trace log */
}                                                     /* Return from send_heartbeat */

static void task_finish(TaskControlBlock *t, uint64_t t_start, const char *dl_msg) { /* Rationale: Cycle finalizer and real-time deadline auditor */
    uint32_t exec = (uint32_t)(get_time_us() - t_start); /* Why: Compute exact cycle execution duration in microseconds */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire task lock */
    t->last_exec_us = exec;                           /* Record execution time of latest cycle */
    t->total_cycles++;                                /* Increment cumulative cycle counter */
    if (exec > t->max_exec_us) t->max_exec_us = exec;  /* Why: Benchmarks Worst-Case Execution Time (WCET) */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    trace_record(t->task_id, EVENT_TASK_END, exec);   /* Record task completion in trace buffer */
    if (!t->inject_starvation) {                      /* Starvation simulation bypass check */
        send_heartbeat(t);                            /* Emit regular heartbeat pulse if not suppressed */
    }                                                 /* End starvation check */
    if (exec > t->deadline_ms * 1000U) {              /* Real-Time Deadline Rule: Exec > Deadline_ms * 1000 */
        RT_MUTEX_LOCK(&g.task_mutex);                 /* Lock to increment miss counter */
        t->deadline_misses++;                         /* Why: Increment missed deadline metric */
        RT_MUTEX_UNLOCK(&g.task_mutex);               /* Release lock */
        fault_register(t->task_id, FAULT_DEADLINE_MISS, SEV_WARNING, dl_msg); /* Raise deadline miss warning */
    } else {                                          /* Completed within allotted real-time budget */
        fault_clear(t->task_id, FAULT_DEADLINE_MISS); /* Self-healing: clear deadline fault */
    }                                                 /* End deadline check */
}                                                     /* Return from task_finish */

static void *service_a_thread(void *arg) {            /* Rationale: High-Priority Real-Time Task 1: Traffic Signal Controller */
    (void)arg;                                        /* Suppress unused parameter warning */
    TaskControlBlock *t = &g.tasks[0];                /* Pointer to Service A TCB (Period=100ms, Deadline=80ms) */
    uint32_t seq = 0;                                 /* Monotonic message sequence counter */
    while (g.running) {                               /* Continuous real-time execution loop */
        uint64_t t_start = get_time_us();             /* Monotonic cycle start timestamp */
        trace_record(t->task_id, EVENT_TASK_START, 0);/* Log start event in trace buffer */
        sleep_ms(15 + t->inject_exec_delay_ms);       /* Why: Simulate nominal 15ms work plus any synthetic injected delay */
        uint64_t snd0 = get_time_us();                /* Record timestamp prior to dispatching IPC to Service B */
        seq++;                                        /* Advance message sequence number */
        if (g.ipc_inject_delay_ms > 0) {              /* Synthetic IPC latency injection check */
            sleep_ms(g.ipc_inject_delay_ms);          /* Sleep for the injected IPC delay */
        }                                             /* End IPC delay check */
        uint32_t lat = (uint32_t)(get_time_us() - snd0); /* Compute round-trip message transit latency */
        ipc_record_latency(lat);                      /* Update rolling statistics and check SLA limits */
        trace_record(t->task_id, EVENT_IPC_XFER, lat);/* Log IPC exchange event with latency metric */
        task_finish(t, t_start, "Service A exceeded 80 ms deadline"); /* Validate deadline budget and emit heartbeat */
        sleep_ms(t->period_ms > 20 ? t->period_ms - 20 : 10); /* Why: Rate-monotonic pacing: sleep for remainder of 100ms period */
    }                                                 /* End service loop */
    return NULL;                                      /* Clean exit */
}                                                     /* Return from service_a_thread */

static void *service_b_thread(void *arg) {            /* Rationale: Medium-Priority Real-Time Task 2: Power Grid Analytics */
    (void)arg;                                        /* Suppress unused parameter warning */
    TaskControlBlock *t = &g.tasks[1];                /* Pointer to Service B TCB (Period=200ms, Deadline=150ms) */
    while (g.running) {                               /* Continuous execution loop */
        uint64_t t_start = get_time_us();             /* Cycle start timestamp */
        trace_record(t->task_id, EVENT_TASK_START, 0);/* Log start in trace buffer */
        sleep_ms(25 + t->inject_exec_delay_ms);       /* Why: Simulate nominal 25ms grid calculation plus delay */
        task_finish(t, t_start, "Service B exceeded 150 ms deadline"); /* Check 150ms deadline and emit heartbeat */
        sleep_ms(t->period_ms > 30 ? t->period_ms - 30 : 10); /* Why: Sleep for remainder of 200ms period */
    }                                                 /* End service loop */
    return NULL;                                      /* Clean exit */
}                                                     /* Return from service_b_thread */

static void *service_c_thread(void *arg) {            /* Rationale: Low-Priority Periodic Task 3: Environmental Telemetry Sampling */
    (void)arg;                                        /* Suppress unused parameter warning */
    TaskControlBlock *t = &g.tasks[2];                /* Pointer to Service C TCB (Period=500ms, Deadline=400ms) */
    while (g.running) {                               /* Continuous execution loop */
        uint64_t t_start = get_time_us();             /* Cycle start timestamp */
        trace_record(t->task_id, EVENT_TASK_START, 0);/* Log start in trace buffer */
        sleep_ms(40 + t->inject_exec_delay_ms);       /* Why: Simulate nominal 40ms environmental sensor acquisition plus delay */
        task_finish(t, t_start, "Service C exceeded 400 ms deadline"); /* Check 400ms deadline and emit heartbeat */
        sleep_ms(t->period_ms > 50 ? t->period_ms - 50 : 20); /* Why: Sleep for remainder of 500ms period */
    }                                                 /* End service loop */
    return NULL;                                      /* Clean exit */
}                                                     /* Return from service_c_thread */

static void *cpu_burner_thread(void *arg) {           /* Rationale: Background synthetic CPU burner for testing overload detection */
    (void)arg;                                        /* Suppress unused parameter warning */
    volatile double d = 0.0;                          /* Why: Volatile accumulator prevents GCC optimizer from removing busy calculations */
    while (g.running) {                               /* Continuous loop */
        if (g.inject_cpu_overload) {                  /* Check if synthetic overload is active */
            uint64_t s = get_time_us();               /* Record start of busy-wait slice */
            while (get_time_us() - s < 80000ULL) {    /* Why: Burn CPU intensely for 80 milliseconds (80% duty cycle) */
                d += sin(d + 1.2345);                 /* Math calculation forces ALU and FPU instruction pipelines */
            }                                         /* End busy loop */
            sleep_ms(20);                             /* Relinquish CPU for 20ms to allow lower priority tasks to breathe */
        } else {                                      /* Overload inactive */
            sleep_ms(200);                            /* Idle sleep while inactive */
        }                                             /* End check */
    }                                                 /* End thread loop */
    return NULL;                                      /* Exit */
}                                                     /* Return from cpu_burner_thread */

/*
 * Rationale: monitoring_task_thread() serves as Node 1's local watchdog and health inspector.
 * Running every 100 ms (MONITOR_INTERVAL_MS) at PRIORITY_MONITOR (20), it audits task liveness,
 * detects thread starvation before the Supervisor even sees it, and computes duty cycle CPU metrics.
 */
static void *monitoring_task_thread(void *arg) {      /* Local health and starvation watchdog thread */
    (void)arg;                                        /* Suppress unused parameter warning */
    /* Why: prev_time stores monotonic reference point to calculate dynamic monitoring window duration */
    uint64_t prev_time = get_time_us();               /* Record initial timestamp of previous window */
    while (g.running) {                               /* Run monitor loop continuously */
        sleep_ms(MONITOR_INTERVAL_MS);                /* Wait for monitor sampling period (100 ms) */
        uint64_t now = get_time_us();                 /* Capture current monotonic timestamp */
        /* Why: window_us measures actual elapsed time since last check, absorbing any scheduler jitter */
        uint64_t window_us = now - prev_time;         /* Calculate elapsed duration of monitoring window */
        prev_time = now;                              /* Advance previous timestamp to current time */
        float work_us = 0.0f;                         /* Accumulator for calculated active work time */
        
        /* Why: Loop audits each of the 3 RT tasks against hard temporal liveness bounds */
        for (int i = 0; i < MAX_TASKS; i++) {         /* Iterate through all registered tasks */
            TaskControlBlock *t = &g.tasks[i];        /* Obtain pointer to task control block */
            /* Why: age_ms calculates milliseconds elapsed since the thread's last successful execution */
            uint64_t age_ms = (now - t->last_heartbeat_time_us) / 1000; /* Calculate heartbeat age in ms */
            
            /* Why: Rule 1 - If task misses 3 consecutive periods (>= 3*T), declare CRITICAL starvation */
            if (age_ms >= t->period_ms * 3) {         /* Check if heartbeat missed 3 consecutive periods */
                char desc[64];                        /* Buffer to construct descriptive fault string */
                t->state = TASK_STATE_STARVED;        /* Transition task state to Starved */
                snprintf(desc, sizeof(desc), "Task '%s' STARVED", t->name); /* Format fault message */
                fault_register(t->task_id, FAULT_TASK_STARVATION, SEV_CRITICAL, desc); /* Register critical */
            /* Why: Rule 2 - If task misses 2 consecutive periods (>= 2*T), raise early WARNING */
            } else if (age_ms >= t->period_ms * 2) {  /* Check if heartbeat missed 2 consecutive periods */
                char desc[64];                        /* Buffer to format warning message */
                t->state = TASK_STATE_WARNING;        /* Transition task state to Warning */
                snprintf(desc, sizeof(desc), "Task '%s' HB slow", t->name); /* Format warning text */
                fault_register(t->task_id, FAULT_TASK_STARVATION, SEV_WARNING, desc); /* Register warning */
            /* Why: Rule 3 - If heartbeat resumed and task was previously degraded, auto-recover to RUNNING */
            } else if (t->state == TASK_STATE_STARVED || t->state == TASK_STATE_WARNING) { /* If now healthy */
                t->state = TASK_STATE_RUNNING;        /* Restore task state to Running */
                fault_clear(t->task_id, FAULT_TASK_STARVATION); /* Clear task starvation fault */
            }                                         /* End liveness evaluation */
            
            /* Why: Duty cycle = (Execution Time / Period) * 100%, clamped to 100% to represent CPU occupancy */
            if (window_us > 0) {                      /* If window duration is valid non-zero */
                float duty = ((float)t->last_exec_us / (float)(t->period_ms * 1000)) * 100.0f; /* Compute duty */
                t->cpu_usage_pct = (duty > 100.0f) ? 100.0f : duty; /* Clamp duty cycle to 100% maximum */
                work_us += t->cpu_usage_pct * 0.01f * (float)window_us; /* Accumulate weighted task busy time */
            }                                         /* End window check */
        }                                             /* End tasks loop */
        
        /* Why: Compute total system workload percentage across active monitoring window */
        float cpu = (window_us > 0) ? (work_us / (float)window_us * 100.0f) : 0.0f; /* Compute CPU percentage */
        /* Why: If synthetic CPU burner is toggled, add artificial 60% load to trigger alarm thresholds */
        if (g.inject_cpu_overload) cpu += 60.0f;      /* Add artificial 60% load if CPU burner is active */
        if (cpu > 100.0f) cpu = 100.0f;               /* Clamp total CPU usage percentage to 100% max */
        g.total_cpu_pct = cpu;                        /* Store computed total CPU in global context */
        
        /* Why: Enforce two-stage CPU threshold watchdog (70% warning, 85% critical safety boundary) */
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

/*
 * Rationale: fill_telemetry() serializes Node 1's entire state into a TelemetryMsg packet.
 * To guarantee strict atomicity and prevent race conditions with executing real-time threads,
 * it locks fine-grained mutexes (ipc_mutex, task_mutex, fault_mutex, trace_mutex) sequentially.
 */
static void fill_telemetry(TelemetryMsg *m) {         /* Package current state into TelemetryMsg packet */
    memset(m, 0, sizeof(*m));                         /* Zero-initialize the entire packet memory */
    m->type         = SC_MSG_TELEMETRY;               /* Set packet type identifier */
    m->node_id      = 1;                              /* Set node identifier (1 for Node 1) */
    m->timestamp_us = get_time_us();                  /* Stamp current microsecond transmission time */
    m->cpu          = g.total_cpu_pct;                /* Copy aggregate CPU usage percentage */
    m->led          = (int32_t)g.led_state;           /* Copy current health LED state */
    
    /* Why: Lock ipc_mutex so samples buffer and P95 statistics are not modified mid-copy */
    RT_MUTEX_LOCK(&g.ipc_mutex);                      /* Acquire lock to read IPC statistics */
    m->ipc          = g.ipc_stats;                    /* Copy snapshot of IPC statistics */
    RT_MUTEX_UNLOCK(&g.ipc_mutex);                    /* Release IPC lock */
    
    /* Why: Lock task_mutex to snapshot all 3 TCBs atomically without torn cycle counters */
    RT_MUTEX_LOCK(&g.task_mutex);                     /* Acquire lock to copy task control blocks */
    for (int i = 0; i < MAX_TASKS; i++) m->tasks[i] = g.tasks[i]; /* Copy all 3 task control blocks */
    RT_MUTEX_UNLOCK(&g.task_mutex);                   /* Release task lock */
    
    /* Why: Lock fault_mutex to preserve consistency between active faults array and fault_counter */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire lock to copy fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) m->fault_map[i] = g.fault_map[i]; /* Copy fault table */
    m->fault_counter = g.fault_counter;               /* Copy lifetime fault counter */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
    
    /* Why: Lock trace_mutex to capture recent execution events without ring buffer wrap corruption */
    RT_MUTEX_LOCK(&g.trace_mutex);                    /* Acquire lock to copy trace buffer */
    m->trace_buf = g.trace_buf;                       /* Copy ring buffer snapshot */
    RT_MUTEX_UNLOCK(&g.trace_mutex);                  /* Release trace lock */
    
    m->inject_cpu_overload = g.inject_cpu_overload;   /* Copy current CPU overload injection flag */
    m->ipc_inject_delay_ms = g.ipc_inject_delay_ms;   /* Copy current IPC latency injection delay */
}                                                     /* Return from fill_telemetry */

/*
 * Rationale: handle_remote_command() executes diagnostic and fault-injection directives
 * issued by the Control Supervisor operator, demonstrating fault resilience under test.
 */
static void handle_remote_command(const ControlCmd *cmd) { /* Process remote command from Supervisor */
    if (!cmd || cmd->type == CMD_NONE) return;        /* Return immediately if no command received */
    printf("[RemoteCmd] Received command type=%d task=%u val=%u\n", cmd->type, cmd->task_id, cmd->value); /* Log */
    switch (cmd->type) {                              /* Dispatch on command classification */
    case CMD_CLEAR_ALL:                               /* Command to reset all active faults and injections */
        /* Why: Clears all active faults and resets injection flags to return to nominal GREEN state */
        fault_clear_all();                            /* Call clear-all handler */
        break;                                        /* End case */
    case CMD_INJECT_STARVE:                           /* Command to simulate starvation on target task */
        /* Why: Suppresses task heartbeat loop so local watchdog and supervisor detect starvation */
        if (cmd->task_id >= 1 && cmd->task_id <= MAX_TASKS) { /* Validate task index range */
            RT_MUTEX_LOCK(&g.task_mutex);             /* Acquire task lock */
            g.tasks[cmd->task_id - 1].inject_starvation = (cmd->value != 0); /* Set starvation flag */
            RT_MUTEX_UNLOCK(&g.task_mutex);           /* Release task lock */
        }                                             /* End validation check */
        break;                                        /* End case */
    case CMD_INJECT_DEADLINE:                         /* Command to inject processing deadline delay */
        /* Why: Inserts busy-wait loop into target task so exec_us exceeds deadline_ms, testing deadline audits */
        if (cmd->task_id >= 1 && cmd->task_id <= MAX_TASKS) { /* Validate task index range */
            RT_MUTEX_LOCK(&g.task_mutex);             /* Acquire task lock */
            g.tasks[cmd->task_id - 1].inject_exec_delay_ms = cmd->value; /* Set execution delay */
            RT_MUTEX_UNLOCK(&g.task_mutex);           /* Release task lock */
        }                                             /* End validation check */
        break;                                        /* End case */
    case CMD_INJECT_CPU:                              /* Command to toggle synthetic CPU burner */
        /* Why: Activates cpu_burner_thread high-frequency arithmetic loop to trigger CPU overload alarms */
        g.inject_cpu_overload = (cmd->value != 0);    /* Set CPU overload flag based on value */
        break;                                        /* End case */
    case CMD_INJECT_IPC:                              /* Command to set injected IPC latency */
        /* Why: Artificially stalls Service A -> Service B data path to test IPC P95 latency alerts */
        g.ipc_inject_delay_ms = cmd->value;           /* Update IPC artificial delay in ms */
        break;                                        /* End case */
    default:                                          /* Unknown command type */
        break;                                        /* Do nothing */
    }                                                 /* End switch statement */
}                                                     /* Return from command handler */

/*
 * Rationale: telemetry_sender_thread() manages inter-node TCP communication to Supervisor.
 * Running at PRIORITY_TELEMETRY (10), it periodically streams telemetry packets every 500 ms
 * and synchronously receives acknowledgments that piggyback remote operator commands.
 */
static void *telemetry_sender_thread(void *arg) {     /* Dispatches telemetry and receives Supervisor commands */
    (void)arg;                                        /* Suppress unused parameter warning */
    int sock = -1;                                    /* File descriptor for TCP connection */
    TelemetryMsg tm;                                  /* Buffer for outgoing telemetry packet */
    TelemetryAck ack;                                 /* Buffer for incoming Supervisor acknowledgment */
    while (g.running) {                               /* Run continuously while application is active */
        sleep_ms(TELEMETRY_INTERVAL_MS);              /* Wait for telemetry reporting period */
        fill_telemetry(&tm);                          /* Package current node telemetry snapshot */
        
        /* Why: Auto-reconnect pattern ensures Node 1 survives supervisor restarts or temporary network drops */
        if (sock < 0) {                               /* If socket is not currently connected */
            sock = socket(AF_INET, SOCK_STREAM, 0);   /* Create standard TCP socket */
            if (sock < 0) continue;                   /* If socket creation failed, retry next loop */
            
            /* Why: SO_RCVTIMEO and SO_SNDTIMEO prevent network hangs from blocking the telemetry thread indefinitely */
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
        
        /* Why: Send binary TelemetryMsg and await TelemetryAck containing any pending operator commands */
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

/*
 * Rationale: main() initializes the Node 1 workload execution runtime.
 * It configures Rate-Monotonic timing parameters (T, D, Priority) for all smart city services,
 * initializes 4 fine-grained POSIX mutexes, launches real-time threads, and monitors lifecycle.
 */
int main(int argc, char *argv[]) {                    /* Application entry point for Node 1 */
    /* Why: Ignore SIGPIPE so broken network socket disconnects do not abort the RTOS application */
    signal(SIGPIPE, SIG_IGN);                         /* Ignore SIGPIPE signals caused by broken sockets */
    memset(&g, 0, sizeof(g));                         /* Zero-initialize global context */
    g.running         = true;                         /* Set active execution flag */
    g.led_state       = LED_GREEN;                    /* Start with healthy GREEN LED status */
    g.supervisor_port = TELEMETRY_PORT;               /* Assign default telemetry TCP port */
    strncpy(g.supervisor_ip, "169.254.178.16", sizeof(g.supervisor_ip) - 1); /* Default supervisor IP */
    bool enable_gpio = true;                          /* Enable simulated GPIO by default */
    
    /* Why: Parse CLI arguments allowing dynamic reconfiguration of network target and hardware emulation */
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
    
    /* Why: Initialize 4 dedicated mutexes to guard task TCBs, IPC metrics, fault registry, and trace logs */
    RT_MUTEX_INIT(&g.task_mutex);                     /* Initialize task control block mutex */
    RT_MUTEX_INIT(&g.ipc_mutex);                      /* Initialize IPC metrics mutex */
    RT_MUTEX_INIT(&g.fault_mutex);                    /* Initialize fault registry mutex */
    RT_MUTEX_INIT(&g.trace_mutex);                    /* Initialize trace ring buffer mutex */
    
    /*
     * Why: Rate-Monotonic Scheduling Configuration:
     * Shorter Period (T) -> Higher Priority.
     * Task 1 (Traffic): T=100ms, D=80ms,  Priority=15 (Highest workload priority)
     * Task 2 (Grid):    T=200ms, D=150ms, Priority=14
     * Task 3 (Env):     T=500ms, D=400ms, Priority=13
     */
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
    
    /* Why: Launch all workload tasks, monitor watchdog, burner, and network telemetry threads */
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
    
    /* Why: rt_thread_join blocks main thread until network telemetry worker exits */
    rt_thread_join(th_tcp);                           /* Wait on telemetry thread loop */
    
    /* Why: Clean teardown sequence stops all threads, yields for completion, and releases HAL resources */
    g.running = false;                                /* Signal all threads to terminate upon exit */
    sleep_ms(300);                                    /* Allow background threads time to exit cleanly */
    if (enable_gpio) hal_gpio_deinit();               /* Deinitialize GPIO subsystem */
    return 0;                                         /* Return success code */
}                                                     /* End of main function */
