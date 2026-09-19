/*
 * control_supervisor.c - Smart City Control Plane Supervisor Node (Pi 2).
 * Operates as the central coordinator, TCP telemetry ingest server, health watchdog,
 * and unified diagnostic CLI for monitoring and remotely controlling edge workload nodes.
 */

#include "cluster_protocol.h"  /* Include shared structures, definitions, and utilities */

extern int   hal_gpio_init(void);                     /* Declare external GPIO initialization routine */
extern void  hal_gpio_set_led(HealthLedState state);  /* Declare external routine to update LED indicator */
extern void *hal_gpio_poll_thread(void *arg);         /* Declare external GPIO input polling thread */
extern void  hal_gpio_deinit(void);                   /* Declare external GPIO cleanup routine */

typedef struct {                                      /* Rationale: Centralized singleton context consolidating all cluster telemetry, locks, and states */
    volatile bool    running;                         /* Why: Marked volatile so compiler prevents register caching; coordinates graceful cluster shutdown */
    uint32_t         node_id;                         /* Why: Topology identifier (Node 2 = Supervisor); distinguishes local logs from edge logs */
    int              tcp_port;                        /* Why: Configurable network endpoint for incoming telemetry connections from Edge Node 1 */
    TaskControlBlock remote_tasks[MAX_TASKS];         /* Why: Local shadow mirror of Node 1's real-time tasks (Service A, B, C) for deadline auditing */
    float            remote_cpu_pct;                  /* Why: Latest CPU load percentage reported over TCP by Node 1 */
    HealthLedState   remote_led;                      /* Why: Mirrored visual health state (Green/Yellow/Red) currently displayed on Node 1 */
    float            local_cpu_pct;                   /* Why: Instantaneous CPU usage of supervisor control plane */
    float            local_cpu_history[100];          /* Why: Fixed-size circular buffer tracking the 100 most recent local CPU samples */
    uint32_t         local_cpu_hist_head;             /* Why: Insertion write-index for the local CPU circular history buffer */
    uint32_t         local_cpu_hist_count;            /* Why: Active sample count (saturates at 100) before wraparound */
    float            local_cpu_min;                   /* Why: Lowest local CPU sample recorded; benchmarks baseline idle load */
    float            local_cpu_max;                   /* Why: Peak local CPU consumption observed; captures transient processing spikes */
    float            local_cpu_avg;                   /* Why: Arithmetic mean of CPU consumption over the 100-sample window */
    float            local_cpu_p95;                   /* Why: 95th percentile CPU consumption; filters out outliers while detecting sustained load */
    uint32_t         packets_received;                /* Why: Cumulative valid telemetry packets ingested; monitors socket throughput and liveness */
    float            cpu_history[100];                /* Why: Rolling circular buffer tracking remote Node 1 CPU utilization samples */
    uint32_t         cpu_hist_head;                   /* Why: Insertion write-index for remote Node 1 CPU circular buffer */
    uint32_t         cpu_hist_count;                  /* Why: Number of valid Node 1 CPU samples received so far */
    float            cpu_min;                         /* Why: Minimum CPU observed on edge worker */
    float            cpu_max;                         /* Why: Peak CPU observed on edge worker (captures computational bottlenecks) */
    float            cpu_avg;                         /* Why: Rolling average CPU load on edge worker */
    float            cpu_p95;                         /* Why: 95th percentile CPU load on edge worker for SLA compliance */
    IpcStats         remote_ipc;                      /* Why: Shadow mirror of Node 1's IPC round-trip latency statistics (Service A -> Service B) */
    FaultRecord      fault_map[MAX_ACTIVE_FAULTS];    /* Why: Statically allocated active fault registry (prevents dynamic memory fragmentation) */
    uint32_t         fault_counter;                   /* Why: Monotonically increasing lifetime fault counter; demonstrates self-healing resilience */
    rt_mutex_t       fault_mutex;                     /* Why: POSIX mutex guarding fault_map against race conditions between watchdog and CLI */
    HealthLedState   led_state;                       /* Why: Local supervisor health LED status driven by highest active fault severity */
    bool             node1_connected;                 /* Why: Real-time link state flag; true when active TCP telemetry socket is maintained */
    uint64_t         node1_last_rx_us;                /* Why: Monotonic timestamp of latest packet; audited by watchdog to detect silent link drops */
    TraceRingBuffer  remote_trace;                    /* Why: Chronological event trace ring buffer (Task Start, End, Heartbeat, Faults) */
    rt_mutex_t       analytics_mutex;                 /* Why: Fine-grained lock protecting telemetry shadow mirrors from torn reads during CLI rendering */
    ControlCmd       pending_cmd;                     /* Why: Outbound command staging buffer (e.g. inject fault, clear faults) dispatched over TCP */
    rt_mutex_t       cmd_mutex;                       /* Why: Mutex synchronizing command staging between operator CLI and TCP transmitter */
    bool             sim_disconnect;                  /* Why: Software simulation flag to test supervisor reaction to severed edge link */
    bool             sim_ipc_timeout;                 /* Why: Software simulation flag to verify supervisor alerting on IPC deadline expiration */
    bool             sim_cpu_overload;                /* Why: Software simulation flag to test automated response to runaway CPU usage */
} SupervisorContext;                                  /* End of SupervisorContext structure */

static SupervisorContext g;                           /* Global singleton instance: zero-initialized in BSS segment */

static void hal_set_led(HealthLedState state) {       /* Rationale: Centralized hardware health state transition dispatcher */
    if (g.led_state == state) return;                 /* Why: Optimization: suppresses redundant hardware register writes and log spam */
    g.led_state = state;                              /* Why: Updates internal context so all diagnostic threads see current health */
    hal_gpio_set_led(state);                          /* Why: Dispatches state to Hardware Abstraction Layer for physical/virtual LED control */
}                                                     /* Return from hal_set_led */

static const char *hal_led_str(HealthLedState state) { /* Rationale: ANSI color-coded string converter for human-readable diagnostic display */
    switch (state) {                                  /* Evaluate discrete health enumeration */
    case LED_GREEN:  return "\033[1;32m[GREEN - HEALTHY]\033[0m";   /* Why: Bold Green indicating zero unresolved faults */
    case LED_YELLOW: return "\033[1;33m[YELLOW - WARNING]\033[0m";  /* Why: Bold Yellow indicating non-fatal deadline misses or elevated latency */
    case LED_RED:    return "\033[1;31m[RED - CRITICAL]\033[0m";    /* Why: Bold Red indicating node disconnect, starvation, or severe timeout */
    default:         return "[?]";                                  /* Fallback for uninitialized state */
    }                                                 /* End switch */
}                                                     /* Return from hal_led_str */

static void fault_register(uint32_t node_id, uint32_t task_id, FaultType type, FaultSeverity sev, const char *desc) { /* Rationale: Thread-safe fault ingestion and LED escalation */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Guard fault table from concurrent access */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* De-duplication scan: check if this exact fault is already active */
        if (g.fault_map[i].active &&                  /* Active slot */
            g.fault_map[i].node_id == node_id &&      /* Same originating node */
            g.fault_map[i].task_id == task_id &&      /* Same target task */
            g.fault_map[i].type    == type) {         /* Same fault classification */
            g.fault_map[i].timestamp_us = get_time_us(); /* Why: Refresh timestamp so operator knows fault is persisting without filling new slots */
            RT_MUTEX_UNLOCK(&g.fault_mutex);          /* Release lock */
            return;                                   /* Exit to prevent redundant duplicate fault entries */
        }                                             /* End matching check */
    }                                                 /* End scan */
    int slot = -1;                                    /* Search for an available empty slot in the static array */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Scan static table */
        if (!g.fault_map[i].active) {                 /* Found an inactive/empty slot */
            slot = i;                                 /* Allocate this slot */
            break;                                    /* Stop search */
        }                                             /* End check */
    }                                                 /* End allocation loop */
    if (slot != -1) {                                 /* Valid slot available */
        g.fault_map[slot].fault_id     = ++g.fault_counter; /* Why: Monotonic ID ensures every fault in history has a unique identifier */
        g.fault_map[slot].node_id      = node_id;     /* Record originating node (1=Edge, 2=Supervisor) */
        g.fault_map[slot].task_id      = task_id;     /* Record affected task ID */
        g.fault_map[slot].type         = type;        /* Record fault category (Starvation, Deadline, CPU, IPC, Disconnect) */
        g.fault_map[slot].severity     = sev;         /* Record severity rank (Warning vs Critical) */
        g.fault_map[slot].timestamp_us = get_time_us(); /* Why: Monotonic timestamp captures precise moment of anomaly onset */
        g.fault_map[slot].active       = true;        /* Mark slot as actively asserted */
        strncpy(g.fault_map[slot].description, desc, sizeof(g.fault_map[slot].description) - 1); /* Copy diagnostic text */
        if (sev == SEV_CRITICAL) {                    /* Critical fault escalation */
            hal_set_led(LED_RED);                     /* Why: Critical failures immediately override visual indicators to RED */
        } else if (sev == SEV_WARNING && g.led_state != LED_RED) { /* Warning escalation */
            hal_set_led(LED_YELLOW);                  /* Why: Set YELLOW only if not already masked by an existing RED fault */
        }                                             /* End severity check */
    }                                                 /* End slot assignment */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release lock */
}                                                     /* Return from fault_register */

static void fault_clear(uint32_t node_id, uint32_t task_id, FaultType type) { /* Rationale: Self-healing resolver with tiered LED de-escalation */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Guard fault table during modification */
    bool has_crit = false;                            /* Flag to track if any other critical faults remain active */
    bool has_warn = false;                            /* Flag to track if any warning faults remain active */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Scan all table slots */
        if (!g.fault_map[i].active) continue;         /* Ignore inactive slots */
        if (g.fault_map[i].node_id == node_id &&      /* Check matching node */
            g.fault_map[i].task_id == task_id &&      /* Check matching task */
            g.fault_map[i].type    == type) {         /* Check matching fault type */
            g.fault_map[i].active = false;            /* Why: Deactivate target fault (self-healed or mitigated) */
        } else {                                      /* For all surviving faults */
            if (g.fault_map[i].severity == SEV_CRITICAL) has_crit = true; /* Critical fault still persists */
            if (g.fault_map[i].severity == SEV_WARNING)  has_warn = true; /* Warning fault still persists */
        }                                             /* End check */
    }                                                 /* End scan loop */
    if (g.remote_led == LED_RED)         has_crit = true; /* Why: Factor in remote Node 1 health before dropping supervisor LED */
    else if (g.remote_led == LED_YELLOW) has_warn = true; /* Factor in remote warning status */
    if      (has_crit) hal_set_led(LED_RED);          /* Priority 1: Maintain RED if any critical anomaly survives */
    else if (has_warn) hal_set_led(LED_YELLOW);       /* Priority 2: Demote to YELLOW if warnings persist */
    else               hal_set_led(LED_GREEN);        /* Priority 3: Restore GREEN only when system is 100% healthy */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release lock */
}                                                     /* Return from fault_clear */

static void queue_node1_command(ControlCmdType type, uint32_t task_id, uint32_t val) { /* Rationale: Thread-safe control plane command queueing */
    RT_MUTEX_LOCK(&g.cmd_mutex);                      /* Thread-safety: Synchronize queue between operator CLI and TCP dispatch */
    g.pending_cmd.type    = type;                     /* Action verb: CMD_INJECT_STARVE, CMD_INJECT_DEADLINE, CMD_CLEAR_ALL, etc. */
    g.pending_cmd.task_id = task_id;                  /* Target real-time task ID on Node 1 */
    g.pending_cmd.value   = val;                      /* Command payload parameter (e.g. delay in ms or state flag) */
    RT_MUTEX_UNLOCK(&g.cmd_mutex);                    /* Release lock */
    printf("[Supervisor] Queued remote command for Node 1 (type=%d task=%u val=%u)\n", type, task_id, val); /* Operator confirmation */
    if (!g.node1_connected) {                         /* Offline resilience check */
        printf("             Notice: Node 1 is currently offline. Command will be delivered on reconnect.\n"); /* Informs user of delayed dispatch */
    }                                                 /* End offline check */
}                                                     /* Return from queue_node1_command */

static void fault_clear_all(void) {                   /* Rationale: Global cluster-wide fault reset and recovery orchestrator */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Acquire lock to clear entire local fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Iterate through all slots */
        g.fault_map[i].active = false;                /* Deactivate every local fault record */
    }                                                 /* End loop */
    g.sim_disconnect   = false;                       /* Why: Reset manual disconnect injection flag */
    g.sim_ipc_timeout  = false;                       /* Why: Reset manual IPC latency injection flag */
    g.sim_cpu_overload = false;                       /* Why: Reset manual CPU overload injection flag */
    hal_set_led(LED_GREEN);                           /* Why: Immediately restore local supervisor health LED to nominal GREEN */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release lock */
    queue_node1_command(CMD_CLEAR_ALL, 0, 0);         /* Why: Dispatch remote command across TCP to also clear all faults on Node 1 */
}                                                     /* Return from fault_clear_all */

static void analytics_update_local_cpu(float cpu) {   /* Rationale: Computes statistical distribution of supervisor processing load */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Thread-safety: Protect analytics metrics while updating CPU history buffer */
    g.local_cpu_pct = cpu;                            /* Why: Store instantaneous measurement for real-time dashboard display */
    g.local_cpu_history[g.local_cpu_hist_head] = cpu; /* Why: Insert sample into circular buffer for rolling window analytics */
    g.local_cpu_hist_head = (g.local_cpu_hist_head + 1) % 100; /* Why: Advance circular buffer index with automatic modular wraparound */
    if (g.local_cpu_hist_count < 100) g.local_cpu_hist_count++; /* Why: Track valid sample count until the 100-sample window is fully populated */
    float mn = 200.0f;                                /* Initialize minimum sentinel above 100% */
    float mx = 0.0f;                                  /* Initialize maximum tracker */
    float sum = 0.0f;                                 /* Initialize accumulator for arithmetic mean */
    float tmp[100];                                   /* Stack-allocated buffer for sorting without dynamic heap allocation */
    uint32_t n = g.local_cpu_hist_count;              /* Capture stable snapshot of active sample count */
    memcpy(tmp, g.local_cpu_history, sizeof(float) * n); /* Why: Copy to temporary array to sort without corrupting chronological history */
    for (uint32_t i = 0; i < n; i++) {                /* Linear scan for extremes and sum */
        if (tmp[i] < mn) mn = tmp[i];                 /* Benchmark lowest idle CPU */
        if (tmp[i] > mx) mx = tmp[i];                 /* Benchmark peak load spike */
        sum += tmp[i];                                /* Accumulate total */
    }                                                 /* End scan loop */
    for (uint32_t i = 1; i < n; i++) {                /* Insertion sort: Highly efficient for small (N=100) real-time datasets */
        float key = tmp[i];                           /* Element to insert */
        int j = (int)i - 1;                           /* Preceding element index */
        while (j >= 0 && tmp[j] > key) {              /* Shift greater elements to the right */
            tmp[j + 1] = tmp[j];                      /* Shift slot */
            j--;                                      /* Move left */
        }                                             /* End shift */
        tmp[j + 1] = key;                             /* Place key in sorted position */
    }                                                 /* End insertion sort */
    g.local_cpu_min = (n > 0) ? mn : 0.0f;            /* Output lowest CPU observed */
    g.local_cpu_max = mx;                             /* Output peak CPU observed */
    g.local_cpu_avg = (n > 0) ? sum / (float)n : 0.0f;/* Output mean CPU load */
    g.local_cpu_p95 = (n > 0) ? tmp[(uint32_t)(n * 0.95f)] : 0.0f; /* Why: 95th percentile filters out 5% extreme noise while capturing sustained load */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
}                                                     /* Return from local CPU updater */

static void analytics_update_cpu(float cpu) {         /* Rationale: Real-time statistical profiler for remote edge worker CPU telemetry */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Thread-safety: Guard remote analytics buffer from concurrent CLI reading */
    g.cpu_history[g.cpu_hist_head] = cpu;             /* Record sample in remote rolling history */
    g.cpu_hist_head = (g.cpu_hist_head + 1) % 100;    /* Advance circular write head */
    if (g.cpu_hist_count < 100) g.cpu_hist_count++;   /* Count valid samples up to capacity */
    float mn = 200.0f;                                /* Minimum sentinel */
    float mx = 0.0f;                                  /* Maximum sentinel */
    float sum = 0.0f;                                 /* Sum accumulator */
    float tmp[100];                                   /* Sorting buffer */
    uint32_t n = g.cpu_hist_count;                    /* Read current count */
    memcpy(tmp, g.cpu_history, sizeof(float) * n);    /* Safe copy for sorting */
    for (uint32_t i = 0; i < n; i++) {                /* Accumulate and scan */
        if (tmp[i] < mn) mn = tmp[i];                 /* Record minimum */
        if (tmp[i] > mx) mx = tmp[i];                 /* Record maximum */
        sum += tmp[i];                                /* Add to sum */
    }                                                 /* End accumulation */
    for (uint32_t i = 1; i < n; i++) {                /* In-place insertion sort */
        float key = tmp[i];                           /* Key element */
        int j = (int)i - 1;                           /* Previous element index */
        while (j >= 0 && tmp[j] > key) {              /* Shift loop */
            tmp[j + 1] = tmp[j];                      /* Shift value */
            j--;                                      /* Decrement pointer */
        }                                             /* End shift */
        tmp[j + 1] = key;                             /* Insert key */
    }                                                 /* End sort */
    g.cpu_min = (n > 0) ? mn : 0.0f;                  /* Min remote CPU */
    g.cpu_max = mx;                                   /* Peak remote CPU */
    g.cpu_avg = (n > 0) ? sum / (float)n : 0.0f;      /* Mean remote CPU */
    g.cpu_p95 = (n > 0) ? tmp[(uint32_t)(n * 0.95f)] : 0.0f; /* Why: Remote P95 audited by watchdog to flag sustained processor saturation */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
}                                                     /* Return from remote CPU updater */

static void fault_detector_evaluate(void) {           /* Rationale: Autonomous RTOS supervisory health rule engine (Watchdog Core) */
    float cpu = g.remote_cpu_pct;                     /* Read instantaneous edge CPU usage */
    if (g.sim_cpu_overload) {                         /* Operator injection simulation check */
        fault_register(1, 0, FAULT_CPU_OVERLOAD, SEV_WARNING, "[SIM] CPU overload simulated"); /* Injected warning */
    } else if (cpu >= CPU_CRIT_THRESHOLD) {           /* Critical rule: CPU >= 85% */
        char d[64]; snprintf(d, sizeof(d), "Node1 CPU Critical: %.1f%%", cpu); /* Format error */
        fault_register(1, 0, FAULT_CPU_OVERLOAD, SEV_CRITICAL, d); /* Why: Severe processor saturation threatens real-time guarantees */
    } else if (cpu >= CPU_WARN_THRESHOLD) {           /* Warning rule: CPU >= 70% */
        char d[64]; snprintf(d, sizeof(d), "Node1 CPU Elevated: %.1f%%", cpu); /* Format warning */
        fault_register(1, 0, FAULT_CPU_OVERLOAD, SEV_WARNING, d); /* Why: Elevated load warning allows preventative operator intervention */
    } else {                                          /* Nominal condition: CPU < 70% */
        fault_clear(1, 0, FAULT_CPU_OVERLOAD);        /* Why: Autonomous self-healing: clears fault when CPU load subsides */
    }                                                 /* End CPU overload rule */

    for (int i = 0; i < MAX_TASKS; i++) {             /* Audit each mirrored real-time task */
        TaskControlBlock *t = &g.remote_tasks[i];     /* Pointer to shadow TCB */
        if (t->task_id == 0) continue;                /* Skip unpopulated slots */
        if (t->state == TASK_STATE_STARVED) {         /* Starvation rule: task missed periodic heartbeats */
            char d[64]; snprintf(d, sizeof(d), "Node1 Task '%s' STARVED", t->name); /* Diagnostic label */
            fault_register(1, t->task_id, FAULT_TASK_STARVATION, SEV_CRITICAL, d); /* Why: Thread starvation is a safety-critical failure */
        } else if (t->state == TASK_STATE_WARNING) {  /* Liveness jitter rule */
            char d[64]; snprintf(d, sizeof(d), "Node1 Task '%s' HB Warning", t->name); /* Diagnostic label */
            fault_register(1, t->task_id, FAULT_TASK_STARVATION, SEV_WARNING, d); /* Why: Heartbeat jitter warning */
        } else {                                      /* Heartbeats regular */
            fault_clear(1, t->task_id, FAULT_TASK_STARVATION); /* Self-healing: clear starvation fault */
        }                                             /* End starvation rule */

        if (t->deadline_ms > 0 && t->last_exec_us > t->deadline_ms * 1000U) { /* Deadline rule: exec > deadline_ms * 1000 */
            char d[64]; snprintf(d, sizeof(d), "Node1 Task '%s' Deadline Miss (%u us)", t->name, t->last_exec_us); /* Format */
            fault_register(1, t->task_id, FAULT_DEADLINE_MISS, SEV_WARNING, d); /* Why: Missed deadline violates deterministic schedule */
        } else {                                      /* Execution time within budget */
            fault_clear(1, t->task_id, FAULT_DEADLINE_MISS); /* Self-healing: clear deadline fault */
        }                                             /* End deadline rule */
    }                                                 /* End tasks audit loop */

    if (g.sim_ipc_timeout) {                          /* Operator IPC timeout injection check */
        fault_register(1, 2, FAULT_IPC_TIMEOUT, SEV_CRITICAL, "[SIM] IPC timeout simulated"); /* Injected fault */
    } else if (g.remote_ipc.p95_us >= IPC_CRIT_LATENCY_US) { /* Critical rule: IPC latency >= 300 ms */
        fault_register(1, 2, FAULT_IPC_TIMEOUT, SEV_CRITICAL, "Node1 IPC Latency Critical"); /* Severe communication bottleneck */
    } else if (g.remote_ipc.p95_us >= IPC_WARN_LATENCY_US) { /* Warning rule: IPC latency >= 220 ms */
        fault_register(1, 2, FAULT_IPC_TIMEOUT, SEV_WARNING, "Node1 IPC Latency Elevated"); /* Elevated message transit delay */
    } else {                                          /* Latency nominal */
        fault_clear(1, 2, FAULT_IPC_TIMEOUT);         /* Self-healing: clear IPC fault */
    }                                                 /* End IPC latency rule */

    if (g.sim_disconnect) {                           /* Operator disconnect injection check */
        g.node1_connected = false;                    /* Mark link disconnected */
        fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "[SIM] Node 1 disconnect simulated"); /* Log */
    } else {                                          /* Watchdog transport audit */
        uint64_t age_ms = (get_time_us() - g.node1_last_rx_us) / 1000; /* Why: Compute elapsed time since last valid telemetry frame */
        if (g.node1_last_rx_us > 0 && age_ms > 3000) {/* 3-second silence threshold */
            g.node1_connected = false;                /* Declare edge link down */
            fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "Node 1 telemetry timeout (>3s)"); /* Critical disconnect fault */
        } else if (g.node1_last_rx_us > 0) {          /* Packets arriving within 3s */
            fault_clear(1, 0, FAULT_NODE_DISCONNECTED); /* Self-healing: restore connection status */
        }                                             /* End age evaluation */
    }                                                 /* End disconnect rule */

    if (g.remote_led == LED_RED) {                    /* Remote escalation rule */
        hal_set_led(LED_RED);                         /* Why: Remote critical failure propagates to central supervisor LED */
    } else if (g.remote_led == LED_YELLOW && g.led_state != LED_RED) { /* Remote warning rule */
        hal_set_led(LED_YELLOW);                      /* Propagate warning if not masked by critical fault */
    }                                                 /* End LED propagation */
}                                                     /* Return from fault_detector_evaluate */

static void ingest_telemetry(const TelemetryMsg *tm) { /* Rationale: Real-time telemetry deserializer and state synchronization engine */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Thread-safety: Atomic lock ensures CLI never reads torn structures */
    g.remote_cpu_pct = tm->cpu;                       /* Copy edge CPU percentage */
    g.remote_led     = (HealthLedState)tm->led;       /* Copy edge hardware LED state */
    g.remote_ipc     = tm->ipc;                       /* Copy inter-service IPC latency metrics */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Deserialization loop for task control blocks */
        g.remote_tasks[i] = tm->tasks[i];             /* Synchronize local shadow TCB with latest edge metrics */
    }                                                 /* End tasks sync */
    g.remote_trace   = tm->trace_buf;                 /* Synchronize trace event ring buffer */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
    g.packets_received++;                             /* Increment packet counter for throughput monitoring */
    g.node1_last_rx_us = get_time_us();               /* Reset watchdog timer with current monotonic timestamp */
    if (!g.sim_disconnect) g.node1_connected = true;  /* Mark link active upon successful frame reception */
    analytics_update_cpu(g.remote_cpu_pct);           /* Update rolling statistics with newly arrived CPU sample */
    fault_detector_evaluate();                        /* Immediately execute supervisory rules against new telemetry */
}                                                     /* Return from ingest_telemetry */

static void *telemetry_server_thread(void *arg) {     /* Rationale: Dedicated TCP server thread managing remote edge node network transport */
    (void)arg;                                        /* Suppress unused parameter warning */
    int srv = socket(AF_INET, SOCK_STREAM, 0);        /* Why: Create an IPv4 streaming TCP socket */
    if (srv < 0) {                                    /* Error check */
        perror("socket");                             /* Log socket creation error */
        return NULL;                                  /* Terminate thread cleanly */
    }                                                 /* End socket check */
    int opt = 1;                                      /* Option flag for SO_REUSEADDR */
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); /* Why: Prevents 'Address already in use' bind errors upon immediate supervisor restarts */
    struct sockaddr_in addr;                          /* Server IPv4 socket address structure */
    memset(&addr, 0, sizeof(addr));                   /* Zero memory */
    addr.sin_family      = AF_INET;                   /* Set IPv4 address family */
    addr.sin_addr.s_addr = INADDR_ANY;                /* Why: Bind to 0.0.0.0 so telemetry can be accepted over Ethernet, Wi-Fi, or localhost */
    addr.sin_port        = htons((uint16_t)g.tcp_port); /* Why: Convert port to network byte order (Big Endian) */
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) { /* Bind socket to port */
        perror("bind");                               /* Log bind error */
        close(srv);                                   /* Clean up file descriptor */
        return NULL;                                  /* Exit */
    }                                                 /* End bind check */
    listen(srv, 2);                                   /* Why: Configure connection backlog (small queue appropriate for dedicated edge-to-supervisor link) */
    printf("[Supervisor] TCP telemetry server listening on port %d...\n", g.tcp_port); /* Confirmation log */
    while (g.running) {                               /* Outer accept loop: automatically accepts reconnects if Edge Node reboots */
        struct sockaddr_in cli_addr;                  /* Client address container */
        socklen_t cli_len = sizeof(cli_addr);         /* Size of address */
        int conn = accept(srv, (struct sockaddr *)&cli_addr, &cli_len); /* Block until Edge Node 1 initiates connection */
        if (conn < 0) continue;                       /* Retry on spurious accept interruption */
        struct timeval tv = { 1, 500000 };            /* 1.5 second socket timeout threshold */
        setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); /* Why: Socket recv timeout prevents infinite hang if client crashes mid-packet */
        setsockopt(conn, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)); /* Why: Socket send timeout prevents hanging supervisor on broken transmit */
        printf("\n\033[1;32m[Supervisor] Node 1 connected from %s\033[0m\n", inet_ntoa(cli_addr.sin_addr)); /* Green connection alert */
        printf("supervisor> "); fflush(stdout);       /* Reprint CLI prompt */
        if (!g.sim_disconnect) {                      /* If disconnect simulation is inactive */
            g.node1_connected = true;                 /* Set link state connected */
            fault_clear(1, 0, FAULT_NODE_DISCONNECTED); /* Self-healing: clear prior disconnect fault */
        }                                             /* End check */
        g.node1_last_rx_us = get_time_us();           /* Update reception timestamp */
        TelemetryMsg tm;                              /* Deserialization buffer for incoming telemetry message */
        TelemetryAck ack;                             /* Serialization buffer for outbound command acknowledgment */
        while (g.running) {                           /* Inner streaming loop: processes continuous telemetry frames */
            int n = recv(conn, &tm, sizeof(tm), MSG_WAITALL); /* Why: MSG_WAITALL blocks until exact full binary struct size is received */
            if (n != (int)sizeof(tm)) break;          /* Detect socket close or partial read error */
            if (tm.type != SC_MSG_TELEMETRY) break;   /* Why: Protocol verification: rejects malformed or unaligned packets */
            ingest_telemetry(&tm);                    /* Atomically ingest packet into local shadow mirrors and evaluate health rules */
            memset(&ack, 0, sizeof(ack));             /* Clear response structure */
            ack.status = EOK;                         /* Set standard POSIX success code */
            RT_MUTEX_LOCK(&g.cmd_mutex);              /* Thread-safety: Lock pending command queue */
            ack.cmd = g.pending_cmd;                  /* Why: Piggyback any queued operator commands inside the ACK packet to avoid separate command sockets */
            g.pending_cmd.type = CMD_NONE;            /* Reset pending command queue to idle state */
            RT_MUTEX_UNLOCK(&g.cmd_mutex);            /* Release command lock */
            if (send(conn, &ack, sizeof(ack), 0) <= 0) break; /* Why: Bidirectional handshake ensures lockstep telemetry/control exchange */
        }                                             /* End streaming loop */
        printf("\n\033[1;31m[Supervisor] Node 1 disconnected.\033[0m\n"); /* Red disconnect alert */
        printf("supervisor> "); fflush(stdout);       /* Reprint CLI prompt */
        g.node1_connected = false;                    /* Mark link inactive */
        fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "Node 1 TCP connection lost"); /* Raise critical cluster fault */
        close(conn);                                  /* Cleanly close client connection socket */
    }                                                 /* End accept loop */
    close(srv);                                       /* Cleanly close server listening socket on shutdown */
    return NULL;                                      /* Exit server thread */
}                                                     /* Return from telemetry_server_thread */

static void update_cpu_display(void) {                /* Rationale: In-place fixed console widget for zero-flicker live CPU visualization */
    static int blink_tick = 0;                        /* Toggle flag for blinking visual heartbeat */
    blink_tick ^= 1;                                  /* Alternate state on every refresh cycle (1 Hz toggle) */
    const char *pulse = blink_tick ? "\033[1;32m● LIVE\033[0m" : "\033[2;32m○ LIVE\033[0m"; /* Why: Pulsing beacon proves monitor loop is not deadlocked */

    printf("\0337");                                  /* ANSI code: Save current operator cursor position */
    printf("\033[12;1H\033[2K==================== CPU UTILIZATION ANALYTICS [%s] ====================\n", pulse); /* Fixed row 12 header with live pulse */
    printf("\033[13;1H\033[2K--- NODE 2 (Supervisor Local) ---\n");                                          /* Fixed row 13: Local supervisor header */
    printf("\033[14;1H\033[2K Current : \033[1;32m%5.1f %%\033[0m\n", g.local_cpu_pct);                         /* Fixed row 14: Local CPU percentage */
    printf("\033[15;1H\033[2K Avg/P95 : %5.1f / %5.1f %%\n", g.local_cpu_avg, g.local_cpu_p95);                 /* Fixed row 15: Local average and 95th percentile */
    printf("\033[16;1H\033[2K\n");                                                                               /* Fixed row 16: Spacer line */
    printf("\033[17;1H\033[2K--- NODE 1 (Workload Remote) %s ---\n",                                            /* Fixed row 17: Remote edge worker header */
           g.node1_connected ? "\033[1;32m[ONLINE]\033[0m" : "\033[1;31m[OFFLINE]\033[0m");                   /* Color-coded connection status */
    printf("\033[18;1H\033[2K Current : \033[1;32m%5.1f %%\033[0m\n", g.remote_cpu_pct);                        /* Fixed row 18: Remote CPU percentage */
    printf("\033[19;1H\033[2K Avg/P95 : %5.1f / %5.1f %%\n", g.cpu_avg, g.cpu_p95);                             /* Fixed row 19: Remote average and 95th percentile */
    printf("\033[20;1H\033[2K====================================================================");            /* Fixed row 20: Delimiter border */
    printf("\0338");                                  /* ANSI code: Restore operator cursor position seamlessly */
    fflush(stdout);                                   /* Flush buffer to display in-place update */
}                                                     /* Return from update_cpu_display */

static void *supervisor_monitor_thread(void *arg) {   /* Rationale: Independent high-priority health watchdog and hardware telemetry sampler */
    (void)arg;                                        /* Suppress unused parameter warning */
    while (g.running) {                               /* Continuous watchdog execution loop */
        sleep_ms(500);                                /* Why: Runs on fixed 500ms period (2 Hz frequency) for predictable monitoring */
        float jitter = ((float)(rand() % 10) - 5.0f) * 0.08f; /* Simulate small variance in local CPU load */
        float base   = g.node1_connected ? 2.4f : 1.6f;       /* Select base workload depending on active network ingestion */
        float sample = base + jitter;                 /* Compute simulated supervisor control plane CPU usage */
        if (sample < 0.8f) sample = 0.8f;             /* Clamp lower bound */
        analytics_update_local_cpu(sample);           /* Update local rolling statistical history */
        uint64_t now = get_time_us();                 /* Capture current monotonic timestamp */
        if (g.sim_disconnect) {                       /* Check if operator simulated network drop */
            g.node1_connected = false;                /* Mark offline */
            fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "[SIM] Node 1 disconnect simulated"); /* Register simulated disconnect */
        } else if (g.node1_last_rx_us > 0) {          /* Node 1 has connected previously */
            uint64_t age_ms = (now - g.node1_last_rx_us) / 1000; /* Calculate time elapsed since last received frame */
            if (age_ms > 2500) {                      /* Why: 2.5 second silence threshold declares node failure */
                g.node1_connected = false;            /* Mark offline */
                fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "Node 1 disconnected (>2.5s silent)"); /* Raise critical alarm */
            }                                         /* End age evaluation */
        }                                             /* End check */
        update_cpu_display();                         /* Update live in-place CPU telemetry display */
    }                                                 /* End watchdog loop */
    return NULL;                                      /* Exit monitor thread */
}                                                     /* Return from supervisor_monitor_thread */

/* =========================================================================
 * Comprehensive CLI Routines (Including full Node 1 diagnostics and controls)
 * ========================================================================= */

static void cli_status(void) {                        /* Rationale: Provides a unified single-pane-of-glass dashboard for cluster health */
    uint32_t active = 0;                              /* Track current unmitigated faults requiring immediate attention */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Prevent race conditions while background monitor updates fault table */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Audit all active slots to distinguish active anomalies from cleared history */
        if (g.fault_map[i].active) active++;          /* Count only unresolved faults affecting system integrity */
    }                                                 /* Complete fault table audit */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release lock immediately to avoid blocking telemetry ingest threads */
    printf("\n===================================================================\n"); /* Visual demarcation for operator readability */
    printf("   SMART CITY RTOS MONITOR -- UNIFIED SYSTEM STATUS\n");                    /* Dashboard title defining scope of monitoring */
    printf("===================================================================\n"); /* Visual boundary */
    printf(" Supervisor LED  : %s\n", hal_led_str(g.led_state));                         /* Why: Shows local supervisor node health independent of edge node */
    printf(" Remote Node1 LED: %s\n", hal_led_str(g.remote_led));                        /* Why: Correlates edge workload health to detect node-level desynchronization */
    printf(" Local CPU (N2)  : %.1f %%\n", g.local_cpu_pct);                             /* Why: Ensures supervisor has adequate headroom to process telemetry and commands */
    printf(" Remote CPU (N1) : %.1f %%\n", g.remote_cpu_pct);                            /* Why: Identifies computational hotspots and runaway tasks on edge worker */
    printf(" Node 1 Link     : %s\n",                                                     /* Why: Instant failover indicator showing network transport connectivity */
           g.node1_connected ? "\033[32mCONNECTED\033[0m" : "\033[31mDISCONNECTED\033[0m"); /* ANSI color coding for zero-latency operator perception */
    printf(" Telemetry Port  : %d\n", g.tcp_port);                                        /* Why: Confirms active ingress networking endpoint for remote telemetry packets */
    printf(" Packets Received: %u\n", g.packets_received);                                /* Why: Verifies continuous telemetry ingestion rate and detects silent connection drops */
    printf(" Active Faults   : %u (lifetime seen: %u)\n", active, g.fault_counter);       /* Why: Compares ongoing system pressure against total cumulative historical anomalies */
    printf(" System Uptime   : " FMT_U64 " ms\n", (uint64_t)(get_time_us() / 1000));     /* Why: Clock drift & liveness baseline computed from monotonic clock source */
    printf("===================================================================\n\n"); /* Footer demarcation */
}                                                     /* Return from cli_status */

static void cli_nodes(void) {                         /* Rationale: Verifies cluster transport topology and edge liveness */
    printf("\n--- NODE CONNECTIVITY STATUS ---\n");    /* Visual section header for network status */
    printf(" Node 1 (Workload)   : %s\n",              /* Why: Identifies remote worker node status in dual-node cluster */
           g.node1_connected ? "\033[32mONLINE\033[0m" : "\033[31mOFFLINE\033[0m"); /* Green ONLINE or Red OFFLINE via ANSI codes */
    printf(" Node 2 (Supervisor) : LOCAL (Listening on TCP port %d)\n", g.tcp_port); /* Why: Confirms local control plane listening socket */
    if (g.node1_last_rx_us) {                         /* Check if at least one telemetry packet has been received */
        uint64_t age = (get_time_us() - g.node1_last_rx_us) / 1000; /* Why: Measures telemetry packet silence age to detect frozen links */
        printf(" Last Packet Rx  : " FMT_U64 " ms ago\n", (uint64_t)age); /* Why: Heartbeat age indicator for watchdog threshold auditing */
        printf(" Total Packets   : %u\n", g.packets_received);           /* Why: Verifies continuous packet throughput over time */
    }                                                 /* End telemetry presence check */
    printf("\n");                                     /* Spacer newline */
}                                                     /* Return from cli_nodes */

static void cli_node1_tasks(void) {                   /* Rationale: Real-time task execution table for rate-monotonic scheduling verification */
    printf("\n=================== NODE 1 WORKLOAD TASK REGISTRY ===================\n"); /* Registry header */
    printf("%-3s %-22s %-9s %-5s %-7s %-7s %-10s %-5s %-5s\n",                       /* Column layout */
           "ID", "TASK NAME", "STATE", "PRIO", "PERIOD", "DEADLN", "EXEC_US", "MISS", "HB"); /* Why: Core real-time scheduling metrics */
    printf("-------------------------------------------------------------------------\n"); /* Table divider */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Thread-safety: Prevent reading torn task blocks during incoming telemetry deserialization */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Iterate through all 3 real-time workload services */
        TaskControlBlock *t = &g.remote_tasks[i];     /* Pointer to locally mirrored task control block */
        if (t->task_id == 0) {                        /* Verify if initial telemetry from edge node has arrived */
            printf("  (Task %d: awaiting telemetry from Node 1)\n", i + 1); /* Inform operator of pending initialization */
            continue;                                 /* Skip uninitialized slot */
        }                                             /* End initialization check */
        const char *st = (t->state == TASK_STATE_STARVED) ? "\033[31mSTARVED\033[0m" : /* Why: Red highlight for starvation failure */
                         (t->state == TASK_STATE_WARNING) ? "\033[33mWARNING\033[0m" : /* Why: Yellow highlight for deadline miss */
                         (t->state == TASK_STATE_RUNNING) ? "RUNNING" : "STOPPED";      /* Normal operational states */
        printf("%-3u %-22s %-9s %-5u %-7u %-7u %-10u %-5u %-5u\n",                   /* Formatted row display */
               t->task_id, t->name, st, t->priority,                                  /* Why: Task identification, state, and priority */
               t->period_ms, t->deadline_ms, t->last_exec_us,                         /* Why: Real-time timing parameters (Period vs Deadline vs Actual Exec) */
               t->deadline_misses, t->heartbeat);                                     /* Why: Fault counts and liveness pulse counter */
    }                                                 /* End tasks loop */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock immediately */
    printf("=========================================================================\n\n"); /* Table boundary */
}                                                     /* Return from cli_node1_tasks */

static void cli_cpu(void) {                           /* Rationale: Comparative processor utilization profiler across cluster nodes */
    printf("\n==================== CPU UTILIZATION ANALYTICS ====================\n"); /* Section header */
    printf("--- NODE 2 (Supervisor Local) ---\n");    /* Local supervisor node domain */
    printf(" Current : %.1f %%\n", g.local_cpu_pct);   /* Why: Real-time instantaneous CPU load of supervisor */
    printf(" Avg/P95 : %.1f / %.1f %%\n\n", g.local_cpu_avg, g.local_cpu_p95); /* Why: 95th percentile load filters out transient spikes */
    printf("--- NODE 1 (Workload Remote) ---\n");     /* Remote edge compute domain */
    printf(" Current : %.1f %%\n", g.remote_cpu_pct);  /* Why: Real-time instantaneous CPU load of edge worker */
    printf(" Avg/P95 : %.1f / %.1f %%\n", g.cpu_avg, g.cpu_p95); /* Why: Statistical baseline to detect sustained overload faults */
    printf("====================================================================\n\n"); /* Section boundary */
}                                                     /* Return from cli_cpu */

static void cli_ipc(void) {                           /* Rationale: Inter-Task message passing latency profiler for real-time IPC */
    printf("\n--- NODE 1 IPC ROUND-TRIP LATENCY (Service A -> Service B) ---\n"); /* Header */
    printf(" Samples Recorded : %u\n", g.remote_ipc.count);                          /* Why: Validates statistical significance of sample window */
    printf(" Min / Max Latency: %u / %u us\n", g.remote_ipc.min_us, g.remote_ipc.max_us); /* Why: Identifies worst-case execution time (WCET) bounds */
    printf(" Avg / P95 Latency: %u / %u us\n", g.remote_ipc.avg_us, g.remote_ipc.p95_us); /* Why: P95 metric identifies tail latency degradation */
    printf(" Latency Limits   : WARN=%u us | CRIT=%u us\n\n",                        /* Why: SLA thresholds determining fault injection triggers */
           IPC_WARN_LATENCY_US, IPC_CRIT_LATENCY_US);                                 /* Explicit threshold constants */
}                                                     /* Return from cli_ipc */

static void cli_faults(void) {                        /* Rationale: Centralized fault containment registry display */
    printf("\n==================== ACTIVE SYSTEM FAULT MAP ====================\n"); /* Table header */
    printf(" Link Status: Node 1 is %s\n\n",                                            /* Current transport layer health */
           g.node1_connected ? "\033[32mONLINE\033[0m" : "\033[31mOFFLINE\033[0m");  /* Color status */
    bool any = false;                                 /* Flag to detect if system is 100% healthy */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Thread-safety: Guard fault table during iteration */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Scan all allocated fault slots */
        FaultRecord *f = &g.fault_map[i];             /* Pointer to individual fault entry */
        if (!f->active) continue;                     /* Ignore resolved or unused slots */
        any = true;                                   /* At least one active fault detected */
        const char *sv = (f->severity == SEV_CRITICAL) ? "\033[31mCRIT\033[0m" : "\033[33mWARN\033[0m"; /* Why: Visual severity tiering */
        printf("  [%s] #%u | Node %u | Task %u | %s\n",                              /* Formatted fault row */
               sv, f->fault_id, f->node_id, f->task_id, f->description);              /* Why: Origin node, affected task, and root cause diagnosis */
    }                                                 /* End fault scan */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
    if (!any) {                                       /* All slots inactive */
        printf("  \033[32mNo active faults detected. System healthy.\033[0m\n");      /* Why: Confirms nominal operational state */
    }                                                 /* End check */
    printf("====================================================================\n\n"); /* Table boundary */
}                                                     /* Return from cli_faults */

static void cli_trace(void) {                         /* Rationale: Monotonic execution trace visualizer for post-mortem timing and jitter analysis */
    printf("\n==================== NODE 1 EVENT TRACE LOG ====================\n"); /* Log section header */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Thread-safety: Prevent trace buffer mutation while rendering event list */
    int n = (int)((g.remote_trace.count > 20) ? 20 : g.remote_trace.count); /* Limit view to the 20 most recent events to prevent console overflow */
    if (n == 0) {                                     /* Check if trace buffer is currently empty */
        printf("  (No trace events recorded yet)\n"); /* Why: Informs operator that event stream is awaiting initial task cycles */
    } else {                                          /* Trace records available */
        int s = (g.remote_trace.head - n + TRACE_BUFFER_SIZE) % TRACE_BUFFER_SIZE; /* Why: Calculate circular buffer head offset for chronological playback */
        for (int i = 0; i < n; i++) {                 /* Iterate chronologically from oldest to newest recorded event */
            TraceRecord *r = &g.remote_trace.records[(s + i) % TRACE_BUFFER_SIZE]; /* Fetch event pointer from ring buffer */
            const char *tn = (r->task_id == 1) ? "Svc_A" : (r->task_id == 2) ? "Svc_B" : "Svc_C"; /* Why: Maps numeric ID to real-time service domain */
            const char *ev = (r->event_type == EVENT_TASK_START) ? "START" :          /* Task dispatch event */
                             (r->event_type == EVENT_TASK_END)   ? "END  " :          /* Task completion event */
                             (r->event_type == EVENT_HEARTBEAT)  ? "HB   " :          /* Watchdog liveness heartbeat */
                             (r->event_type == EVENT_FAULT)      ? "FAULT" : "IPC  "; /* Anomaly or IPC event */
            printf("  +%06lu us | %-5s | %-5s | %u us\n",                             /* Formatted chronological event entry */
                   (unsigned long)(r->timestamp_us % 10000000ULL), tn, ev, r->duration_us); /* Why: Monotonic timestamp delta, task, event type, execution duration */
        }                                             /* End trace loop */
    }                                                 /* End record check */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
    printf("====================================================================\n\n"); /* Footer boundary */
}                                                     /* Return from cli_trace */

static void cli_help(void) {                          /* Display comprehensive help screen */
    printf("\n==================== SUPERVISOR & NODE 1 DIAGNOSTIC CLI ====================\n"); /* Title */
    printf("  status                       - Overall system health summary (both nodes)\n");    /* Command */
    printf("  nodes                        - Node connectivity and link metrics\n");             /* Command */
    printf("  tasks                        - Mirrored Node 1 task execution table\n");           /* Command */
    printf("  cpu                          - CPU utilization analytics for Node 1 and Node 2\n");/* Command */
    printf("  ipc                          - Service A -> Service B IPC round-trip latency\n");  /* Command */
    printf("  faults / faultmap            - Comprehensive active fault registry\n");            /* Command */
    printf("  trace                        - Node 1 recent event trace buffer\n");               /* Command */
    printf("  clear                        - Clear all active faults and reset simulations\n");  /* Command */
    printf("\n--- Remote Node 1 Control & Injections ---\n");                                    /* Section */
    printf("  node1 status                 - Inspect Node 1 specific health state\n");           /* Command */
    printf("  node1 tasks                  - Inspect Node 1 task control blocks\n");             /* Command */
    printf("  node1 cpu                    - Inspect Node 1 CPU utilization\n");                 /* Command */
    printf("  node1 ipc                    - Inspect Node 1 IPC latency statistics\n");          /* Command */
    printf("  node1 faults                 - Inspect Node 1 active faults\n");                   /* Command */
    printf("  node1 trace                  - Inspect Node 1 event trace buffer\n");              /* Command */
    printf("  node1 inject starve <1-3>    - Inject starvation simulation on target task (1-3)\n"); /* Command */
    printf("  node1 inject deadline <1-3> <ms> - Inject execution delay causing deadline miss\n"); /* Command */
    printf("  node1 inject cpu on|off      - Turn synthetic CPU burner on Node 1 on or off\n");  /* Command */
    printf("  node1 inject ipc <ms>        - Inject round-trip latency delay into Node 1 IPC\n");/* Command */
    printf("  node1 clear                  - Clear all faults and injections on Node 1\n");      /* Command */
    printf("\n  exit / quit                  - Shutdown Supervisor node\n");                     /* Command */
    printf("================================================================================\n\n"); /* Footer */
}                                                     /* Return from cli_help */

static void *cli_thread(void *arg) {                  /* Rationale: Interactive diagnostic terminal thread with split-screen ANSI layout */
    (void)arg;                                        /* Suppress unused parameter warning */
    char line[256];                                   /* Line buffer allocated on stack for user input commands */
    printf("\033[2J\033[1;1H");                       /* ANSI codes: Clear entire screen (\033[2J) and home cursor to row 1, col 1 (\033[1;1H) */
    printf("===================================================================\n");
    printf(" %s - NODE 2 (Supervisor Node)\n", PLATFORM_NAME);
    printf(" Telemetry Ingest Port : %d | GPIO Simulation: ON\n", g.tcp_port);
    printf(" Unified CLI: Full Node 1 diagnostics & remote control enabled.\n");
    printf("===================================================================\n");
    printf("Hardware abstraction layer initialized (software simulation mode).\n");
    printf("[Supervisor] TCP telemetry server listening on port %d...\n\n", g.tcp_port);
    printf("*** SMART CITY SUPERVISOR CONSOLE READY ***\n");
    printf("    Type 'help' for available monitoring and remote Node 1 control commands.\n\n");
    update_cpu_display();                             /* Initial render: Draws pinned CPU analytics table in rows 12-20 */
    printf("\033[22;r\033[22;1H");                    /* Why: Pinned split-screen: Restricts scrolling region from row 22 downwards so top tables never scroll off */
    fflush(stdout);                                   /* Flush stdout to activate ANSI terminal scroll margins */

    while (g.running) {                               /* Command processing loop */
        update_cpu_display();                         /* Why: Refresh pinned CPU telemetry table before every prompt to keep live metrics current */
        printf("supervisor> ");                       /* Output interactive command prompt */
        fflush(stdout);                               /* Flush prompt immediately to terminal */
        if (!fgets(line, sizeof(line), stdin)) break; /* Why: fgets reads safe bounded line from stdin; detects EOF (Ctrl+D) to break cleanly */
        line[strcspn(line, "\r\n")] = 0;              /* Why: Strip CR/LF newline characters from input string */
        if (!strlen(line)) continue;                  /* Ignore empty enter presses */
        
        /* Command Dispatcher: Evaluates operator diagnostic and remote injection commands */
        if (!strcmp(line, "help")) {
            cli_help();                               /* Why: Displays operator instruction reference */
        } else if (!strcmp(line, "status") || !strcmp(line, "node1 status")) {
            cli_status();                             /* Why: Single-pane-of-glass cluster health overview */
        } else if (!strcmp(line, "nodes")) {
            cli_nodes();                              /* Why: Network topology and connection age metrics */
        } else if (!strcmp(line, "tasks") || !strcmp(line, "node1 tasks")) {
            cli_node1_tasks();                        /* Why: Real-time task execution table (WCET, periods, deadlines, heartbeats) */
        } else if (!strcmp(line, "cpu") || !strcmp(line, "node1 cpu")) {
            cli_cpu();                                /* Why: Statistical CPU distribution (Current, Avg, P95) */
        } else if (!strcmp(line, "ipc") || !strcmp(line, "node1 ipc")) {
            cli_ipc();                                /* Why: Inter-task message passing round-trip latency profiler */
        } else if (!strcmp(line, "faults") || !strcmp(line, "faultmap") || !strcmp(line, "node1 faults")) {
            cli_faults();                             /* Why: Comprehensive registry of active faults with origin node and severity */
        } else if (!strcmp(line, "trace") || !strcmp(line, "node1 trace")) {
            cli_trace();                              /* Why: Microsecond chronological event trace buffer playback */
        } else if (!strcmp(line, "clear") || !strcmp(line, "node1 clear")) {
            fault_clear_all();                        /* Why: Cluster-wide recovery: clears local faults and dispatches CMD_CLEAR_ALL to Node 1 */
            printf("[CLI] All faults cleared and reset command dispatched.\n");
        } else if (!strncmp(line, "node1 inject starve ", 20) || !strncmp(line, "inject starve ", 14)) {
            const char *p = strstr(line, "starve ") + 7;
            int tid = atoi(p);                        /* Target task identifier (1=Traffic, 2=Grid, 3=Env) */
            if (tid >= 1 && tid <= MAX_TASKS) {
                queue_node1_command(CMD_INJECT_STARVE, (uint32_t)tid, 1); /* Why: Injects thread starvation to demonstrate watchdog detection */
            } else {
                printf("Error: Invalid task ID (choose 1, 2, or 3).\n");
            }
        } else if (!strncmp(line, "node1 inject deadline ", 22) || !strncmp(line, "inject deadline ", 16)) {
            const char *p = strstr(line, "deadline ") + 9;
            int tid = 0, ms = 0;
            if (sscanf(p, "%d %d", &tid, &ms) == 2 && tid >= 1 && tid <= MAX_TASKS) {
                queue_node1_command(CMD_INJECT_DEADLINE, (uint32_t)tid, (uint32_t)ms); /* Why: Injects delay to cause real-time deadline miss */
            } else {
                printf("Usage: node1 inject deadline <1-3> <delay_ms>\n");
            }
        } else if (!strncmp(line, "node1 inject cpu ", 17) || !strncmp(line, "inject cpu ", 11)) {
            const char *p = strstr(line, "cpu ") + 4;
            bool on = (!strcmp(p, "on") || !strcmp(p, "1"));
            queue_node1_command(CMD_INJECT_CPU, 0, on ? 1 : 0); /* Why: Toggles CPU load generator on Node 1 to trigger overload alarms */
        } else if (!strncmp(line, "node1 inject ipc ", 17) || !strncmp(line, "inject ipc ", 11)) {
            const char *p = strstr(line, "ipc ") + 4;
            int ms = atoi(p);
            queue_node1_command(CMD_INJECT_IPC, 0, (uint32_t)ms); /* Why: Artificially inflates IPC transmission latency to test timeout SLA */
        } else if (!strcmp(line, "exit") || !strcmp(line, "quit")) {
            g.running = false;                        /* Why: Signals shutdown across all background worker threads */
            break;                                    /* Break out of CLI execution loop */
        } else {
            printf("Unknown command '%s'. Type 'help' for command list.\n", line);
        }
    }                                                 /* End command processing loop */
    printf("\033[r\033[2J\033[1;1H");                 /* Terminal restoration: Reset scroll region (\033[r) and clear screen on exit */
    fflush(stdout);                                   /* Flush buffer */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from cli_thread */

int main(int argc, char *argv[]) {                    /* Supervisor node entry point: Orchestrates initialization, synchronization, and threads */
    signal(SIGPIPE, SIG_IGN);                         /* Resilience: Prevent OS kernel from abruptly terminating process if remote TCP socket disconnects */
    memset(&g, 0, sizeof(g));                         /* Determinism: Zero out entire global runtime context structure to eliminate uninitialized memory artifacts */
    g.running       = true;                           /* Atomic control flag: Coordinates lifecycle across all concurrent worker threads */
    g.node_id       = 2;                              /* Topology identifier: Configures this machine as Node 2 (Central Control Plane Supervisor) */
    g.led_state     = LED_GREEN;                      /* Initial health state: Assumes clean system startup until anomalies are registered */
    g.tcp_port      = TELEMETRY_PORT;                 /* Networking: Sets standard TCP ingress port (8080) for incoming edge telemetry streams */
    g.cpu_min       = 100.0f;                         /* Analytics baseline: Seeds minimum tracker with high sentinel for rolling statistics */
    g.local_cpu_pct = 2.0f;                           /* Initial local CPU percentage estimate for lightweight supervisor control plane */
    bool enable_gpio = true;                          /* Hardware flag: Controls whether HAL GPIO/LED simulation subsystem is activated */
    for (int i = 1; i < argc; i++) {                  /* Command-line parser: Allows dynamic runtime configuration without recompilation */
        if (!strncmp(argv[i], "--port=", 7)) {        /* User override: Custom TCP port configuration */
            g.tcp_port = atoi(argv[i] + 7);           /* Parse integer port from CLI string */
        }                                             /* End port override */
        if (!strcmp(argv[i], "--no-gpio")) {          /* Headless execution: Disable GPIO emulation for automated testing / CI/CD pipelines */
            enable_gpio = false;                      /* Suppress HAL GPIO initialization and polling thread */
        }                                             /* End GPIO check */
    }                                                 /* End CLI argument parsing */

    /* =========================================================================
     * RT_MUTEX SYNCHRONIZATION: Fine-Grained Locks to Prevent Data Races & Lock Contention
     * ========================================================================= */
    RT_MUTEX_INIT(&g.fault_mutex);                    /* Mutex 1: Guards fault registry (g.fault_map, g.fault_counter) between monitor thread and CLI */
    RT_MUTEX_INIT(&g.analytics_mutex);                /* Mutex 2: Guards telemetry data (g.remote_tasks, g.remote_trace, CPU metrics) from TCP ingest */
    RT_MUTEX_INIT(&g.cmd_mutex);                      /* Mutex 3: Guards outgoing remote command queue (g.pending_cmd) between CLI and TCP transmitter */

    /* =========================================================================
     * TASK CONTROL BLOCKS (TCB): Static Task Descriptor Registry for Node 1 Services
     * ========================================================================= */
    g.remote_tasks[0] = (TaskControlBlock){ .task_id = 1, .period_ms = 100, .deadline_ms = 80,  .state = TASK_STATE_RUNNING }; /* Service A: Period=100ms, Deadline=80ms */
    strncpy(g.remote_tasks[0].name, "Service_A (Traffic)", 31);                                  /* Human-readable identifier for traffic signal controller */
    g.remote_tasks[1] = (TaskControlBlock){ .task_id = 2, .period_ms = 200, .deadline_ms = 150, .state = TASK_STATE_RUNNING }; /* Service B: Period=200ms, Deadline=150ms */
    strncpy(g.remote_tasks[1].name, "Service_B (Grid)", 31);                                     /* Human-readable identifier for power grid monitoring task */
    g.remote_tasks[2] = (TaskControlBlock){ .task_id = 3, .period_ms = 500, .deadline_ms = 400, .state = TASK_STATE_RUNNING }; /* Service C: Period=500ms, Deadline=400ms */
    strncpy(g.remote_tasks[2].name, "Service_C (Env)", 31);                                      /* Human-readable identifier for environmental telemetry task */

    if (enable_gpio) hal_gpio_init();                 /* Initialize simulated GPIO subsystem: configures virtual pins and sets initial GREEN LED */

    /* =========================================================================
     * RT_THREADS: Real-Time Concurrent Execution Threads with Priority Assignment
     * ========================================================================= */
    rt_thread_t th_tcp, th_mon, th_cli, th_gpio;      /* Native POSIX thread handles (pthread_t) aliased for real-time portability */
    
    /* Thread 1: Central Health Watchdog - Audits heartbeats, evaluates deadlines, and updates cluster health status */
    rt_thread_create(&th_mon, PRIORITY_MONITOR, supervisor_monitor_thread, NULL);   /* High Priority: Safety-critical monitoring watchdog */
    
    /* Thread 2: TCP Ingest Server - Listens on TCP port, deserializes edge packets, and queues outbound control commands */
    rt_thread_create(&th_tcp, PRIORITY_TELEMETRY, telemetry_server_thread, NULL);   /* High Priority: Real-time network telemetry intake */
    
    if (enable_gpio) {                                /* Conditional thread launch based on hardware simulation flag */
        /* Thread 3: HAL GPIO Poller - Samples physical/simulated button inputs with 50ms software debouncing */
        rt_thread_create(&th_gpio, PRIORITY_GPIO, hal_gpio_poll_thread, NULL);       /* Medium Priority: Asynchronous input polling */
    }                                                 /* End GPIO poller launch */
    
    /* Thread 4: Interactive Diagnostic Shell - Handles operator commands, diagnostic tables, and fault injection */
    rt_thread_create(&th_cli, PRIORITY_CLI, cli_thread, NULL);                       /* Low Priority: Non-blocking operator diagnostic interface */

    /* =========================================================================
     * SYSTEM LIFECYCLE & TEARDOWN: Clean Termination Sequence
     * ========================================================================= */
    rt_thread_join(th_cli);                           /* Synchronization barrier: Block main thread until operator issues 'exit' command in CLI */
    g.running = false;                                /* Teardown broadcast: Invalidate running flag to trigger graceful exit loops across all threads */
    printf("\033[r");                                 /* Terminal restoration: Reset VT100 scroll margins back to full console view */
    fflush(stdout);                                   /* I/O sync: Flush standard output buffer to ensure all trailing messages are displayed */
    sleep_ms(300);                                    /* Grace period: Allow background network and monitoring threads to complete cleanup */
    if (enable_gpio) hal_gpio_deinit();               /* Safe hardware state: Turn off all simulated LEDs and reset pin driver status */
    return 0;                                         /* Return EXIT_SUCCESS to host operating system */
}                                                     /* End of main function */
