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

typedef struct {                                      /* Supervisor node global runtime context */
    volatile bool    running;                         /* Global execution flag controlling thread loops */
    uint32_t         node_id;                         /* Local node identifier (value 2) */
    int              tcp_port;                        /* TCP port for incoming Node 1 telemetry connections */
    TaskControlBlock remote_tasks[MAX_TASKS];         /* Mirrored status of tasks running on Node 1 */
    float            remote_cpu_pct;                  /* Latest CPU percentage reported by Node 1 */
    HealthLedState   remote_led;                      /* Latest health LED state reported by Node 1 */
    float            local_cpu_pct;                   /* Supervisor local simulated CPU utilization */
    float            local_cpu_history[100];          /* Rolling history of local CPU measurements */
    uint32_t         local_cpu_hist_head;             /* Insertion index for local CPU history */
    uint32_t         local_cpu_hist_count;            /* Count of valid local CPU samples */
    float            local_cpu_min;                   /* Lowest local CPU sample observed */
    float            local_cpu_max;                   /* Peak local CPU sample observed */
    float            local_cpu_avg;                   /* Average local CPU consumption */
    float            local_cpu_p95;                   /* 95th percentile local CPU consumption */
    uint32_t         packets_received;                /* Count of valid telemetry packets received */
    float            cpu_history[100];                /* Rolling history of remote Node 1 CPU samples */
    uint32_t         cpu_hist_head;                   /* Insertion index for remote CPU history */
    uint32_t         cpu_hist_count;                  /* Count of valid remote CPU samples */
    float            cpu_min;                         /* Minimum remote CPU utilization recorded */
    float            cpu_max;                         /* Maximum remote CPU utilization recorded */
    float            cpu_avg;                         /* Average remote CPU utilization recorded */
    float            cpu_p95;                         /* 95th percentile remote CPU utilization */
    IpcStats         remote_ipc;                      /* Mirrored IPC latency stats from Node 1 */
    FaultRecord      fault_map[MAX_ACTIVE_FAULTS];    /* Unified fault registry for Supervisor and Node 1 */
    uint32_t         fault_counter;                   /* Monotonic sequential counter for faults */
    rt_mutex_t       fault_mutex;                     /* Mutex protecting fault table access */
    HealthLedState   led_state;                       /* Supervisor local health LED indicator state */
    bool             node1_connected;                 /* Boolean indicating active connection to Node 1 */
    uint64_t         node1_last_rx_us;                /* Monotonic timestamp of last received packet */
    TraceRingBuffer  remote_trace;                    /* Mirrored trace ring buffer from Node 1 */
    rt_mutex_t       analytics_mutex;                 /* Mutex protecting analytics and telemetry mirror */
    ControlCmd       pending_cmd;                     /* Next command queued to be sent to Node 1 */
    rt_mutex_t       cmd_mutex;                       /* Mutex guarding the pending command queue */
    bool             sim_disconnect;                  /* Flag to simulate Node 1 disconnection */
    bool             sim_ipc_timeout;                 /* Flag to simulate IPC timeout condition */
    bool             sim_cpu_overload;                /* Flag to simulate CPU overload condition */
} SupervisorContext;                                  /* End of SupervisorContext structure */

static SupervisorContext g;                           /* Global singleton instance of supervisor context */

static void hal_set_led(HealthLedState state) {       /* Update local supervisor health LED indicator */
    if (g.led_state == state) return;                 /* Avoid redundant calls if state hasn't changed */
    g.led_state = state;                              /* Save LED state into supervisor context */
    hal_gpio_set_led(state);                          /* Pass updated state to GPIO HAL */
}                                                     /* Return from LED updater */

static const char *hal_led_str(HealthLedState state) { /* Convert LED state to color-coded string */
    switch (state) {                                  /* Match LED state value */
    case LED_GREEN:  return "\033[1;32m[GREEN - HEALTHY]\033[0m";   /* Return formatted green label */
    case LED_YELLOW: return "\033[1;33m[YELLOW - WARNING]\033[0m";  /* Return formatted yellow label */
    case LED_RED:    return "\033[1;31m[RED - CRITICAL]\033[0m";    /* Return formatted red label */
    default:         return "[?]";                                  /* Return fallback for unknown state */
    }                                                 /* End switch */
}                                                     /* Return from LED string formatter */

static void fault_register(uint32_t node_id, uint32_t task_id, FaultType type, FaultSeverity sev, const char *desc) { /* Register fault */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire exclusive lock on fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Scan registry for existing active fault */
        if (g.fault_map[i].active &&                  /* Check if slot contains an active fault */
            g.fault_map[i].node_id == node_id &&      /* Check if node matches */
            g.fault_map[i].task_id == task_id &&      /* Check if task matches */
            g.fault_map[i].type    == type) {         /* Check if fault category matches */
            g.fault_map[i].timestamp_us = get_time_us(); /* Refresh timestamp on repeating fault */
            RT_MUTEX_UNLOCK(&g.fault_mutex);          /* Release lock before returning */
            return;                                   /* Exit without creating duplicate entry */
        }                                             /* End matching condition */
    }                                                 /* End search loop */
    int slot = -1;                                    /* Slot index variable */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Search for an available inactive slot */
        if (!g.fault_map[i].active) {                 /* If slot is empty */
            slot = i;                                 /* Select this slot index */
            break;                                    /* Terminate search */
        }                                             /* End check */
    }                                                 /* End slot search loop */
    if (slot != -1) {                                 /* If a valid slot was found */
        g.fault_map[slot].fault_id     = ++g.fault_counter; /* Assign incremental fault identifier */
        g.fault_map[slot].node_id      = node_id;     /* Assign reporting node identifier */
        g.fault_map[slot].task_id      = task_id;     /* Assign associated task identifier */
        g.fault_map[slot].type         = type;        /* Assign fault category */
        g.fault_map[slot].severity     = sev;         /* Assign severity rating */
        g.fault_map[slot].timestamp_us = get_time_us(); /* Record detection time */
        g.fault_map[slot].active       = true;        /* Flag entry as actively asserted */
        strncpy(g.fault_map[slot].description, desc, sizeof(g.fault_map[slot].description) - 1); /* Copy description */
        if (sev == SEV_CRITICAL) {                    /* If severity is critical */
            hal_set_led(LED_RED);                     /* Set supervisor LED to RED */
        } else if (sev == SEV_WARNING && g.led_state != LED_RED) { /* If warning and LED is not RED */
            hal_set_led(LED_YELLOW);                  /* Set supervisor LED to YELLOW */
        }                                             /* End severity check */
    }                                                 /* End slot assignment */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release exclusive lock on fault registry */
}                                                     /* Return from fault registration function */

static void fault_clear(uint32_t node_id, uint32_t task_id, FaultType type) { /* Clear active fault */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire exclusive lock on fault registry */
    bool has_crit = false;                            /* Flag indicating presence of critical faults */
    bool has_warn = false;                            /* Flag indicating presence of warning faults */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Loop over all slots in fault table */
        if (!g.fault_map[i].active) continue;         /* Skip inactive entries */
        if (g.fault_map[i].node_id == node_id &&      /* Check if node matches target */
            g.fault_map[i].task_id == task_id &&      /* Check if task matches target */
            g.fault_map[i].type    == type) {         /* Check if type matches target */
            g.fault_map[i].active = false;            /* Deactivate the fault record */
        } else {                                      /* For all remaining active faults */
            if (g.fault_map[i].severity == SEV_CRITICAL) has_crit = true; /* Note persisting critical */
            if (g.fault_map[i].severity == SEV_WARNING)  has_warn = true; /* Note persisting warning */
        }                                             /* End match check */
    }                                                 /* End loop */
    if (g.remote_led == LED_RED)         has_crit = true; /* Factor in remote node critical state */
    else if (g.remote_led == LED_YELLOW) has_warn = true; /* Factor in remote node warning state */
    if      (has_crit) hal_set_led(LED_RED);          /* Set RED if any critical fault is present */
    else if (has_warn) hal_set_led(LED_YELLOW);       /* Set YELLOW if any warning is present */
    else               hal_set_led(LED_GREEN);        /* Set GREEN if all clear */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release lock on fault registry */
}                                                     /* Return from fault clear function */

static void queue_node1_command(ControlCmdType type, uint32_t task_id, uint32_t val) { /* Queue remote command */
    RT_MUTEX_LOCK(&g.cmd_mutex);                      /* Acquire lock on command queue */
    g.pending_cmd.type    = type;                     /* Set command action type */
    g.pending_cmd.task_id = task_id;                  /* Set target task ID */
    g.pending_cmd.value   = val;                      /* Set parameter value */
    RT_MUTEX_UNLOCK(&g.cmd_mutex);                    /* Release command queue lock */
    printf("[Supervisor] Queued remote command for Node 1 (type=%d task=%u val=%u)\n", type, task_id, val); /* Log */
    if (!g.node1_connected) {                         /* If Node 1 is currently offline */
        printf("             Notice: Node 1 is currently offline. Command will be delivered on reconnect.\n"); /* Warn */
    }                                                 /* End connection check */
}                                                     /* Return from queue command function */

static void fault_clear_all(void) {                   /* Clear all faults locally and on Node 1 */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire exclusive lock on fault registry */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Loop through fault table */
        g.fault_map[i].active = false;                /* Deactivate all fault records */
    }                                                 /* End loop */
    g.sim_disconnect   = false;                       /* Reset simulated disconnect flag */
    g.sim_ipc_timeout  = false;                       /* Reset simulated IPC timeout flag */
    g.sim_cpu_overload = false;                       /* Reset simulated CPU overload flag */
    hal_set_led(LED_GREEN);                           /* Restore GREEN LED indicator */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault registry lock */
    queue_node1_command(CMD_CLEAR_ALL, 0, 0);         /* Dispatch clear-all command to Node 1 */
}                                                     /* Return from fault_clear_all */

static void analytics_update_local_cpu(float cpu) {   /* Update local Supervisor CPU statistics */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Acquire lock on analytics state */
    g.local_cpu_pct = cpu;                            /* Store latest local CPU reading */
    g.local_cpu_history[g.local_cpu_hist_head] = cpu; /* Add to rolling history buffer */
    g.local_cpu_hist_head = (g.local_cpu_hist_head + 1) % 100; /* Advance circular buffer index */
    if (g.local_cpu_hist_count < 100) g.local_cpu_hist_count++; /* Increment sample count if below max */
    float mn = 200.0f;                                /* Initialize minimum tracker */
    float mx = 0.0f;                                  /* Initialize maximum tracker */
    float sum = 0.0f;                                 /* Initialize sum accumulator */
    float tmp[100];                                   /* Temporary buffer for sorting percentiles */
    uint32_t n = g.local_cpu_hist_count;              /* Read current count of samples */
    memcpy(tmp, g.local_cpu_history, sizeof(float) * n); /* Copy sample data into temp buffer */
    for (uint32_t i = 0; i < n; i++) {                /* Iterate over all samples */
        if (tmp[i] < mn) mn = tmp[i];                 /* Update minimum value */
        if (tmp[i] > mx) mx = tmp[i];                 /* Update maximum value */
        sum += tmp[i];                                /* Add to sum */
    }                                                 /* End aggregation loop */
    for (uint32_t i = 1; i < n; i++) {                /* Perform insertion sort */
        float key = tmp[i];                           /* Save element to insert */
        int j = (int)i - 1;                           /* Set predecessor index */
        while (j >= 0 && tmp[j] > key) {              /* Shift greater elements */
            tmp[j + 1] = tmp[j];                      /* Shift element right */
            j--;                                      /* Decrement index */
        }                                             /* End shift loop */
        tmp[j + 1] = key;                             /* Insert key into slot */
    }                                                 /* End sort loop */
    g.local_cpu_min = (n > 0) ? mn : 0.0f;            /* Set computed local minimum */
    g.local_cpu_max = mx;                             /* Set computed local maximum */
    g.local_cpu_avg = (n > 0) ? sum / (float)n : 0.0f;/* Set computed local average */
    g.local_cpu_p95 = (n > 0) ? tmp[(uint32_t)(n * 0.95f)] : 0.0f; /* Set computed local 95th percentile */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
}                                                     /* Return from local CPU updater */

static void analytics_update_cpu(float cpu) {         /* Update remote Node 1 CPU statistics */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Acquire lock on analytics state */
    g.cpu_history[g.cpu_hist_head] = cpu;             /* Add sample to remote CPU history buffer */
    g.cpu_hist_head = (g.cpu_hist_head + 1) % 100;    /* Advance circular buffer index */
    if (g.cpu_hist_count < 100) g.cpu_hist_count++;   /* Increment count up to limit */
    float mn = 200.0f;                                /* Initialize minimum tracker */
    float mx = 0.0f;                                  /* Initialize maximum tracker */
    float sum = 0.0f;                                 /* Initialize sum accumulator */
    float tmp[100];                                   /* Temporary buffer for sorting percentiles */
    uint32_t n = g.cpu_hist_count;                    /* Read count of samples */
    memcpy(tmp, g.cpu_history, sizeof(float) * n);    /* Copy samples into temp buffer */
    for (uint32_t i = 0; i < n; i++) {                /* Iterate over all samples */
        if (tmp[i] < mn) mn = tmp[i];                 /* Update minimum value */
        if (tmp[i] > mx) mx = tmp[i];                 /* Update maximum value */
        sum += tmp[i];                                /* Add to sum */
    }                                                 /* End aggregation loop */
    for (uint32_t i = 1; i < n; i++) {                /* Perform insertion sort */
        float key = tmp[i];                           /* Save element to insert */
        int j = (int)i - 1;                           /* Set predecessor index */
        while (j >= 0 && tmp[j] > key) {              /* Shift greater elements */
            tmp[j + 1] = tmp[j];                      /* Shift element right */
            j--;                                      /* Decrement index */
        }                                             /* End shift loop */
        tmp[j + 1] = key;                             /* Insert key into slot */
    }                                                 /* End sort loop */
    g.cpu_min = (n > 0) ? mn : 0.0f;                  /* Set computed remote minimum */
    g.cpu_max = mx;                                   /* Set computed remote maximum */
    g.cpu_avg = (n > 0) ? sum / (float)n : 0.0f;      /* Set computed remote average */
    g.cpu_p95 = (n > 0) ? tmp[(uint32_t)(n * 0.95f)] : 0.0f; /* Set computed remote 95th percentile */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
}                                                     /* Return from remote CPU updater */

static void fault_detector_evaluate(void) {           /* Run supervisor supervisory health rules */
    float cpu = g.remote_cpu_pct;                     /* Read current remote CPU percentage */
    if (g.sim_cpu_overload) {                         /* Check if simulation flag is set */
        fault_register(1, 0, FAULT_CPU_OVERLOAD, SEV_WARNING, "[SIM] CPU overload simulated"); /* Log */
    } else if (cpu >= CPU_CRIT_THRESHOLD) {           /* Check if CPU exceeds critical threshold */
        char d[64]; snprintf(d, sizeof(d), "Node1 CPU Critical: %.1f%%", cpu); /* Format error */
        fault_register(1, 0, FAULT_CPU_OVERLOAD, SEV_CRITICAL, d); /* Register critical fault */
    } else if (cpu >= CPU_WARN_THRESHOLD) {           /* Check if CPU exceeds warning threshold */
        char d[64]; snprintf(d, sizeof(d), "Node1 CPU Elevated: %.1f%%", cpu); /* Format warning */
        fault_register(1, 0, FAULT_CPU_OVERLOAD, SEV_WARNING, d); /* Register warning fault */
    } else {                                          /* CPU is healthy */
        fault_clear(1, 0, FAULT_CPU_OVERLOAD);        /* Clear any CPU overload fault */
    }                                                 /* End CPU check */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Iterate over remote task mirror */
        TaskControlBlock *t = &g.remote_tasks[i];     /* Get pointer to task control block */
        if (t->task_id == 0) continue;                /* Skip uninitialized tasks */
        if (t->state == TASK_STATE_STARVED) {         /* Check if task is reported starved */
            char d[64]; snprintf(d, sizeof(d), "Node1 Task '%s' STARVED", t->name); /* Format */
            fault_register(1, t->task_id, FAULT_TASK_STARVATION, SEV_CRITICAL, d); /* Log critical */
        } else if (t->state == TASK_STATE_WARNING) {  /* Check if task is reported in warning state */
            char d[64]; snprintf(d, sizeof(d), "Node1 Task '%s' HB Warning", t->name); /* Format */
            fault_register(1, t->task_id, FAULT_TASK_STARVATION, SEV_WARNING, d); /* Log warning */
        } else {                                      /* Task is running normally */
            fault_clear(1, t->task_id, FAULT_TASK_STARVATION); /* Clear starvation fault */
        }                                             /* End starvation check */
        if (t->deadline_ms > 0 && t->last_exec_us > t->deadline_ms * 1000U) { /* Deadline miss check */
            char d[64]; snprintf(d, sizeof(d), "Node1 Task '%s' Deadline Miss (%u us)", t->name, t->last_exec_us); /* Format */
            fault_register(1, t->task_id, FAULT_DEADLINE_MISS, SEV_WARNING, d); /* Log deadline fault */
        } else {                                      /* Deadline met */
            fault_clear(1, t->task_id, FAULT_DEADLINE_MISS); /* Clear deadline fault */
        }                                             /* End deadline evaluation */
    }                                                 /* End tasks loop */
    if (g.sim_ipc_timeout) {                          /* Check if IPC timeout simulation is asserted */
        fault_register(1, 2, FAULT_IPC_TIMEOUT, SEV_CRITICAL, "[SIM] IPC timeout simulated"); /* Log */
    } else if (g.remote_ipc.p95_us >= IPC_CRIT_LATENCY_US) { /* Critical latency check */
        fault_register(1, 2, FAULT_IPC_TIMEOUT, SEV_CRITICAL, "Node1 IPC Latency Critical"); /* Log */
    } else if (g.remote_ipc.p95_us >= IPC_WARN_LATENCY_US) { /* Warning latency check */
        fault_register(1, 2, FAULT_IPC_TIMEOUT, SEV_WARNING, "Node1 IPC Latency Elevated"); /* Log */
    } else {                                          /* Latency normal */
        fault_clear(1, 2, FAULT_IPC_TIMEOUT);         /* Clear IPC fault */
    }                                                 /* End IPC evaluation */
    if (g.sim_disconnect) {                           /* Check if disconnect simulation is enabled */
        g.node1_connected = false;                    /* Mark node disconnected */
        fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "[SIM] Node 1 disconnect simulated"); /* Log */
    } else {                                          /* Normal connectivity evaluation */
        uint64_t age_ms = (get_time_us() - g.node1_last_rx_us) / 1000; /* Calculate packet silence age */
        if (g.node1_last_rx_us > 0 && age_ms > 3000) {/* If no telemetry packet received in 3 seconds */
            g.node1_connected = false;                /* Mark disconnected */
            fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "Node 1 telemetry timeout (>3s)"); /* Log */
        } else if (g.node1_last_rx_us > 0) {          /* Connection active and recent */
            fault_clear(1, 0, FAULT_NODE_DISCONNECTED); /* Clear disconnection fault */
        }                                             /* End age check */
    }                                                 /* End disconnect evaluation */
    if (g.remote_led == LED_RED) {                    /* If remote node is in red critical state */
        hal_set_led(LED_RED);                         /* Match local LED to RED */
    } else if (g.remote_led == LED_YELLOW && g.led_state != LED_RED) { /* If remote node is in warning */
        hal_set_led(LED_YELLOW);                      /* Match local LED to YELLOW */
    }                                                 /* End LED update */
}                                                     /* Return from health evaluator */

static void ingest_telemetry(const TelemetryMsg *tm) { /* Ingest received telemetry packet into context */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Acquire lock on analytics */
    g.remote_cpu_pct = tm->cpu;                       /* Update remote CPU reading */
    g.remote_led     = (HealthLedState)tm->led;       /* Update remote LED status */
    g.remote_ipc     = tm->ipc;                       /* Update remote IPC statistics */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Iterate across all 3 tasks */
        g.remote_tasks[i] = tm->tasks[i];             /* Update mirrored task control block */
    }                                                 /* End tasks loop */
    g.remote_trace   = tm->trace_buf;                 /* Update mirrored trace ring buffer */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
    g.packets_received++;                             /* Increment received telemetry packet counter */
    g.node1_last_rx_us = get_time_us();               /* Record timestamp of successful reception */
    if (!g.sim_disconnect) g.node1_connected = true;  /* Mark node as actively connected */
    analytics_update_cpu(g.remote_cpu_pct);           /* Update remote CPU rolling statistics */
    fault_detector_evaluate();                        /* Evaluate health and fault rules */
}                                                     /* Return from telemetry ingestion routine */

static void *telemetry_server_thread(void *arg) {     /* TCP server thread receiving Node 1 connections */
    (void)arg;                                        /* Suppress unused parameter warning */
    int srv = socket(AF_INET, SOCK_STREAM, 0);        /* Create listening TCP socket */
    if (srv < 0) {                                    /* Check if socket creation failed */
        perror("socket");                             /* Print system error message */
        return NULL;                                  /* Terminate thread */
    }                                                 /* End socket check */
    int opt = 1;                                      /* Option value for SO_REUSEADDR */
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); /* Allow immediate port reuse */
    struct sockaddr_in addr;                          /* Server bind address structure */
    memset(&addr, 0, sizeof(addr));                   /* Zero memory */
    addr.sin_family      = AF_INET;                   /* IPv4 protocol family */
    addr.sin_addr.s_addr = INADDR_ANY;                /* Bind to all local interfaces */
    addr.sin_port        = htons((uint16_t)g.tcp_port); /* Set listening TCP port */
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) { /* Bind socket to port */
        perror("bind");                               /* Print bind error */
        close(srv);                                   /* Close socket */
        return NULL;                                  /* Terminate thread */
    }                                                 /* End bind check */
    listen(srv, 2);                                   /* Listen for incoming connections */
    printf("[Supervisor] TCP telemetry server listening on port %d...\n", g.tcp_port); /* Log server start */
    while (g.running) {                               /* Run accept loop while server is active */
        struct sockaddr_in cli_addr;                  /* Client address storage */
        socklen_t cli_len = sizeof(cli_addr);         /* Size of client address */
        int conn = accept(srv, (struct sockaddr *)&cli_addr, &cli_len); /* Accept incoming connection */
        if (conn < 0) continue;                       /* If accept failed, retry */
        struct timeval tv = { 1, 500000 };            /* 1.5 second socket timeout */
        setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); /* Set receive timeout */
        setsockopt(conn, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)); /* Set send timeout */
        printf("\n\033[1;32m[Supervisor] Node 1 connected from %s\033[0m\n", inet_ntoa(cli_addr.sin_addr)); /* Log */
        printf("supervisor> "); fflush(stdout);       /* Reprint CLI prompt */
        if (!g.sim_disconnect) {                      /* If disconnect simulation is not active */
            g.node1_connected = true;                 /* Flag node as connected */
            fault_clear(1, 0, FAULT_NODE_DISCONNECTED); /* Clear disconnected fault */
        }                                             /* End check */
        g.node1_last_rx_us = get_time_us();           /* Update reception timestamp */
        TelemetryMsg tm;                              /* Telemetry packet buffer */
        TelemetryAck ack;                             /* Acknowledgment packet buffer */
        while (g.running) {                           /* Stream packets until client disconnects */
            int n = recv(conn, &tm, sizeof(tm), MSG_WAITALL); /* Receive full telemetry message */
            if (n != (int)sizeof(tm)) break;          /* Break if socket closed or incomplete read */
            if (tm.type != SC_MSG_TELEMETRY) break;   /* Validate message type identifier */
            ingest_telemetry(&tm);                    /* Ingest packet into local mirrors */
            memset(&ack, 0, sizeof(ack));             /* Clear acknowledgment memory */
            ack.status = EOK;                         /* Set success status */
            RT_MUTEX_LOCK(&g.cmd_mutex);              /* Acquire command lock */
            ack.cmd = g.pending_cmd;                  /* Copy queued remote command into ack */
            g.pending_cmd.type = CMD_NONE;            /* Reset pending command queue to idle */
            RT_MUTEX_UNLOCK(&g.cmd_mutex);            /* Release command lock */
            if (send(conn, &ack, sizeof(ack), 0) <= 0) break; /* Dispatch ACK back to Node 1 */
        }                                             /* End packet stream loop */
        printf("\n\033[1;31m[Supervisor] Node 1 disconnected.\033[0m\n"); /* Log disconnect */
        printf("supervisor> "); fflush(stdout);       /* Reprint CLI prompt */
        g.node1_connected = false;                    /* Mark node as disconnected */
        fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "Node 1 TCP connection lost"); /* Log fault */
        close(conn);                                  /* Close client connection socket */
    }                                                 /* End accept loop */
    close(srv);                                       /* Close server listening socket */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from telemetry server thread */

static void update_cpu_display(void) {             /* Continuously update live CPU block in place */
    static int blink_tick = 0;                        /* Toggle flag for blinking heartbeat */
    blink_tick ^= 1;                                  /* Alternate state on every refresh */
    const char *pulse = blink_tick ? "\033[1;32m● LIVE\033[0m" : "\033[2;32m○ LIVE\033[0m"; /* Pulse heartbeat badge */

    printf("\0337");                                  /* ANSI save cursor position */
    printf("\033[12;1H\033[2K==================== CPU UTILIZATION ANALYTICS [%s] ====================\n", pulse); /* Header at line 12 with live pulse */
    printf("\033[13;1H\033[2K--- NODE 2 (Supervisor Local) ---\n");                                          /* Line 13: Node 2 local supervisor header */
    printf("\033[14;1H\033[2K Current : \033[1;32m%5.1f %%\033[0m\n", g.local_cpu_pct);                         /* Line 14: Local CPU percentage in green */
    printf("\033[15;1H\033[2K Avg/P95 : %5.1f / %5.1f %%\n", g.local_cpu_avg, g.local_cpu_p95);                 /* Line 15: Local CPU average and 95th percentile */
    printf("\033[16;1H\033[2K\n");                                                                               /* Line 16: Empty spacer line */
    printf("\033[17;1H\033[2K--- NODE 1 (Workload Remote) %s ---\n",                                            /* Line 17: Node 1 remote header with status */
           g.node1_connected ? "\033[1;32m[ONLINE]\033[0m" : "\033[1;31m[OFFLINE]\033[0m");                   /* Highlight ONLINE (green) or OFFLINE (red) */
    printf("\033[18;1H\033[2K Current : \033[1;32m%5.1f %%\033[0m\n", g.remote_cpu_pct);                        /* Line 18: Remote CPU percentage in green */
    printf("\033[19;1H\033[2K Avg/P95 : %5.1f / %5.1f %%\n", g.cpu_avg, g.cpu_p95);                             /* Line 19: Remote CPU average and 95th percentile */
    printf("\033[20;1H\033[2K====================================================================");            /* Line 20: Bottom border delimiter */
    printf("\0338");                                  /* ANSI Restore cursor position */
    fflush(stdout);                                   /* Flush output buffer */
}

static void *supervisor_monitor_thread(void *arg) {   /* Periodic watchdog and CPU sampler */
    (void)arg;                                        /* Suppress unused parameter warning */
    while (g.running) {                               /* Continuous monitoring loop */
        sleep_ms(500);                                /* Run every 500 milliseconds */
        float jitter = ((float)(rand() % 10) - 5.0f) * 0.08f; /* Generate slight random variance */
        float base   = g.node1_connected ? 2.4f : 1.6f;       /* Select base CPU workload */
        float sample = base + jitter;                 /* Compute simulated local CPU usage */
        if (sample < 0.8f) sample = 0.8f;             /* Clamp minimum value */
        analytics_update_local_cpu(sample);           /* Update local CPU statistics */
        uint64_t now = get_time_us();                 /* Capture current timestamp */
        if (g.sim_disconnect) {                       /* Check if disconnect is simulated */
            g.node1_connected = false;                /* Mark disconnected */
            fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "[SIM] Node 1 disconnect simulated"); /* Log */
        } else if (g.node1_last_rx_us > 0) {          /* If Node 1 was previously heard from */
            uint64_t age_ms = (now - g.node1_last_rx_us) / 1000; /* Calculate time since last packet */
            if (age_ms > 2500) {                      /* If silent for over 2.5 seconds */
                g.node1_connected = false;            /* Mark disconnected */
                fault_register(1, 0, FAULT_NODE_DISCONNECTED, SEV_CRITICAL, "Node 1 disconnected (>2.5s silent)"); /* Log */
            }                                         /* End age check */
        }                                             /* End check */
        update_cpu_display();                         /* Always refresh live CPU utilization block */
    }                                                 /* End thread loop */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from monitor thread */

/* =========================================================================
 * Comprehensive CLI Routines (Including full Node 1 diagnostics and controls)
 * ========================================================================= */

static void cli_status(void) {                        /* Display overall health summary */
    uint32_t active = 0;                              /* Counter for active faults */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire fault lock */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Loop over fault table */
        if (g.fault_map[i].active) active++;          /* Count active entries */
    }                                                 /* End loop */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
    printf("\n===================================================================\n"); /* Banner */
    printf("   SMART CITY RTOS MONITOR -- UNIFIED SYSTEM STATUS\n");                    /* Title */
    printf("===================================================================\n"); /* Divider */
    printf(" Supervisor LED  : %s\n", hal_led_str(g.led_state));                         /* Supervisor LED */
    printf(" Remote Node1 LED: %s\n", hal_led_str(g.remote_led));                        /* Node 1 LED */
    printf(" Local CPU (N2)  : %.1f %%\n", g.local_cpu_pct);                             /* Supervisor CPU */
    printf(" Remote CPU (N1) : %.1f %%\n", g.remote_cpu_pct);                            /* Node 1 CPU */
    printf(" Node 1 Link     : %s\n",                                                     /* Link state */
           g.node1_connected ? "\033[32mCONNECTED\033[0m" : "\033[31mDISCONNECTED\033[0m"); /* Colored */
    printf(" Telemetry Port  : %d\n", g.tcp_port);                                        /* Port */
    printf(" Packets Received: %u\n", g.packets_received);                                /* Count */
    printf(" Active Faults   : %u (lifetime seen: %u)\n", active, g.fault_counter);       /* Faults count */
    printf(" System Uptime   : " FMT_U64 " ms\n", (uint64_t)(get_time_us() / 1000));     /* Uptime in ms */
    printf("===================================================================\n\n"); /* End */
}                                                     /* Return from cli_status */

static void cli_nodes(void) {                         /* Display detailed node connectivity */
    printf("\n--- NODE CONNECTIVITY STATUS ---\n");    /* Section header */
    printf(" Node 1 (Workload)   : %s\n",              /* Node 1 status */
           g.node1_connected ? "\033[32mONLINE\033[0m" : "\033[31mOFFLINE\033[0m"); /* Color state */
    printf(" Node 2 (Supervisor) : LOCAL (Listening on TCP port %d)\n", g.tcp_port); /* Node 2 state */
    if (g.node1_last_rx_us) {                         /* If node 1 has communicated */
        uint64_t age = (get_time_us() - g.node1_last_rx_us) / 1000; /* Calculate elapsed milliseconds */
        printf(" Last Packet Rx  : " FMT_U64 " ms ago\n", (uint64_t)age); /* Print age */
        printf(" Total Packets   : %u\n", g.packets_received);           /* Print total packets */
    }                                                 /* End check */
    printf("\n");                                     /* Newline */
}                                                     /* Return from cli_nodes */

static void cli_node1_tasks(void) {                   /* Display Node 1 task telemetry table */
    printf("\n=================== NODE 1 WORKLOAD TASK REGISTRY ===================\n"); /* Header */
    printf("%-3s %-22s %-9s %-5s %-7s %-7s %-10s %-5s %-5s\n",                       /* Columns */
           "ID", "TASK NAME", "STATE", "PRIO", "PERIOD", "DEADLN", "EXEC_US", "MISS", "HB"); /* Labels */
    printf("-------------------------------------------------------------------------\n"); /* Line */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Acquire analytics lock */
    for (int i = 0; i < MAX_TASKS; i++) {             /* Iterate over all 3 tasks */
        TaskControlBlock *t = &g.remote_tasks[i];     /* Pointer to mirrored task block */
        if (t->task_id == 0) {                        /* If task data has not arrived */
            printf("  (Task %d: awaiting telemetry from Node 1)\n", i + 1); /* Print notice */
            continue;                                 /* Next task */
        }                                             /* End check */
        const char *st = (t->state == TASK_STATE_STARVED) ? "\033[31mSTARVED\033[0m" : /* Starved */
                         (t->state == TASK_STATE_WARNING) ? "\033[33mWARNING\033[0m" : /* Warning */
                         (t->state == TASK_STATE_RUNNING) ? "RUNNING" : "STOPPED";      /* Running */
        printf("%-3u %-22s %-9s %-5u %-7u %-7u %-10u %-5u %-5u\n",                   /* Format row */
               t->task_id, t->name, st, t->priority,                                  /* Core info */
               t->period_ms, t->deadline_ms, t->last_exec_us,                         /* Timing */
               t->deadline_misses, t->heartbeat);                                     /* Fault counters */
    }                                                 /* End tasks loop */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
    printf("=========================================================================\n\n"); /* End */
}                                                     /* Return from cli_node1_tasks */

static void cli_cpu(void) {                           /* Display CPU analytics for both nodes */
    printf("\n==================== CPU UTILIZATION ANALYTICS ====================\n"); /* Header */
    printf("--- NODE 2 (Supervisor Local) ---\n");    /* Supervisor section */
    printf(" Current : %.1f %%\n", g.local_cpu_pct);   /* Current usage */
    printf(" Avg/P95 : %.1f / %.1f %%\n\n", g.local_cpu_avg, g.local_cpu_p95); /* Mean and 95th */
    printf("--- NODE 1 (Workload Remote) ---\n");     /* Node 1 section */
    printf(" Current : %.1f %%\n", g.remote_cpu_pct);  /* Current usage */
    printf(" Avg/P95 : %.1f / %.1f %%\n", g.cpu_avg, g.cpu_p95); /* Mean and 95th */
    printf("====================================================================\n\n"); /* Footer */
}                                                     /* Return from cli_cpu */

static void cli_ipc(void) {                           /* Display IPC round-trip latency statistics */
    printf("\n--- NODE 1 IPC ROUND-TRIP LATENCY (Service A -> Service B) ---\n"); /* Header */
    printf(" Samples Recorded : %u\n", g.remote_ipc.count);                          /* Sample count */
    printf(" Min / Max Latency: %u / %u us\n", g.remote_ipc.min_us, g.remote_ipc.max_us); /* Range */
    printf(" Avg / P95 Latency: %u / %u us\n", g.remote_ipc.avg_us, g.remote_ipc.p95_us); /* Stats */
    printf(" Latency Limits   : WARN=%u us | CRIT=%u us\n\n",                        /* Limits */
           IPC_WARN_LATENCY_US, IPC_CRIT_LATENCY_US);                                 /* Limits values */
}                                                     /* Return from cli_ipc */

static void cli_faults(void) {                        /* Display unified active fault table */
    printf("\n==================== ACTIVE SYSTEM FAULT MAP ====================\n"); /* Header */
    printf(" Link Status: Node 1 is %s\n\n",                                            /* Status */
           g.node1_connected ? "\033[32mONLINE\033[0m" : "\033[31mOFFLINE\033[0m");  /* Color state */
    bool any = false;                                 /* Flag tracking any active faults */
    RT_MUTEX_LOCK(&g.fault_mutex);                    /* Acquire fault lock */
    for (int i = 0; i < MAX_ACTIVE_FAULTS; i++) {     /* Loop across table */
        FaultRecord *f = &g.fault_map[i];             /* Pointer to fault slot */
        if (!f->active) continue;                     /* Skip inactive slots */
        any = true;                                   /* Set found flag */
        const char *sv = (f->severity == SEV_CRITICAL) ? "\033[31mCRIT\033[0m" : "\033[33mWARN\033[0m"; /* Severity tag */
        printf("  [%s] #%u | Node %u | Task %u | %s\n",                              /* Row format */
               sv, f->fault_id, f->node_id, f->task_id, f->description);              /* Values */
    }                                                 /* End loop */
    RT_MUTEX_UNLOCK(&g.fault_mutex);                  /* Release fault lock */
    if (!any) {                                       /* If no faults are active */
        printf("  \033[32mNo active faults detected. System healthy.\033[0m\n");      /* Clean message */
    }                                                 /* End check */
    printf("====================================================================\n\n"); /* End */
}                                                     /* Return from cli_faults */

static void cli_trace(void) {                         /* Display Node 1 recent event trace buffer */
    printf("\n==================== NODE 1 EVENT TRACE LOG ====================\n"); /* Header */
    RT_MUTEX_LOCK(&g.analytics_mutex);                /* Acquire analytics lock */
    int n = (int)((g.remote_trace.count > 20) ? 20 : g.remote_trace.count); /* Show up to 20 events */
    if (n == 0) {                                     /* If no trace events recorded */
        printf("  (No trace events recorded yet)\n"); /* Message */
    } else {                                          /* If records exist */
        int s = (g.remote_trace.head - n + TRACE_BUFFER_SIZE) % TRACE_BUFFER_SIZE; /* Calculate start */
        for (int i = 0; i < n; i++) {                 /* Loop over events */
            TraceRecord *r = &g.remote_trace.records[(s + i) % TRACE_BUFFER_SIZE]; /* Get record */
            const char *tn = (r->task_id == 1) ? "Svc_A" : (r->task_id == 2) ? "Svc_B" : "Svc_C"; /* Task */
            const char *ev = (r->event_type == EVENT_TASK_START) ? "START" :          /* Start */
                             (r->event_type == EVENT_TASK_END)   ? "END  " :          /* End */
                             (r->event_type == EVENT_HEARTBEAT)  ? "HB   " :          /* Heartbeat */
                             (r->event_type == EVENT_FAULT)      ? "FAULT" : "IPC  "; /* Fault/IPC */
            printf("  +%06lu us | %-5s | %-5s | %u us\n",                             /* Row format */
                   (unsigned long)(r->timestamp_us % 10000000ULL), tn, ev, r->duration_us); /* Values */
        }                                             /* End loop */
    }                                                 /* End check */
    RT_MUTEX_UNLOCK(&g.analytics_mutex);              /* Release analytics lock */
    printf("====================================================================\n\n"); /* End */
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

static void *cli_thread(void *arg) {                  /* Diagnostic CLI processing thread */
    (void)arg;                                        /* Suppress unused parameter warning */
    char line[256];                                   /* Input line buffer */
    printf("\033[2J\033[1;1H");                       /* Clear screen and position at line 1 */
    printf("===================================================================\n");
    printf(" %s - NODE 2 (Supervisor Node)\n", PLATFORM_NAME);
    printf(" Telemetry Ingest Port : %d | GPIO Simulation: ON\n", g.tcp_port);
    printf(" Unified CLI: Full Node 1 diagnostics & remote control enabled.\n");
    printf("===================================================================\n");
    printf("Hardware abstraction layer initialized (software simulation mode).\n");
    printf("[Supervisor] TCP telemetry server listening on port %d...\n\n", g.tcp_port);
    printf("*** SMART CITY SUPERVISOR CONSOLE READY ***\n");
    printf("    Type 'help' for available monitoring and remote Node 1 control commands.\n\n");
    update_cpu_display();                             /* Render live CPU utilization block at lines 12-20 */
    printf("\033[22;r\033[22;1H");                    /* Configure scrolling region from line 22 downwards */
    fflush(stdout);

    while (g.running) {                               /* Command processing loop */
        update_cpu_display();                         /* Continuously refresh live CPU status */
        printf("supervisor> ");                       /* Output command prompt */
        fflush(stdout);                               /* Flush prompt immediately */
        if (!fgets(line, sizeof(line), stdin)) break; /* Read user input line from standard input */
        line[strcspn(line, "\r\n")] = 0;              /* Strip trailing newline characters */
        if (!strlen(line)) continue;                  /* Ignore empty input */
        if      (!strcmp(line, "help"))                                    cli_help();           /* Help */
        else if (!strcmp(line, "status") || !strcmp(line, "node1 status")) cli_status();         /* Status */
        else if (!strcmp(line, "nodes"))                                   cli_nodes();          /* Nodes */
        else if (!strcmp(line, "tasks")  || !strcmp(line, "node1 tasks"))  cli_node1_tasks();    /* Tasks */
        else if (!strcmp(line, "cpu")    || !strcmp(line, "node1 cpu"))    cli_cpu();            /* CPU */
        else if (!strcmp(line, "ipc")    || !strcmp(line, "node1 ipc"))    cli_ipc();            /* IPC */
        else if (!strcmp(line, "faults") || !strcmp(line, "faultmap") ||                         /* Faults */
                 !strcmp(line, "node1 faults"))                            cli_faults();         /* Faults */
        else if (!strcmp(line, "trace")  || !strcmp(line, "node1 trace"))  cli_trace();          /* Trace */
        else if (!strcmp(line, "clear")  || !strcmp(line, "node1 clear")) {                      /* Clear */
            fault_clear_all();                                                                   /* Clear all */
            printf("[CLI] All faults cleared and reset command dispatched.\n");                  /* Ack */
        }                                                                                        /* End clear */
        else if (!strncmp(line, "node1 inject starve ", 20) ||                                   /* Starve */
                 !strncmp(line, "inject starve ", 14)) {                                         /* Short */
            const char *p = strstr(line, "starve ") + 7;                                         /* Value */
            int tid = atoi(p);                                                                   /* Task ID */
            if (tid >= 1 && tid <= MAX_TASKS) {                                                  /* Range check */
                queue_node1_command(CMD_INJECT_STARVE, (uint32_t)tid, 1);                        /* Queue cmd */
            } else {                                                                             /* Invalid */
                printf("Error: Invalid task ID (choose 1, 2, or 3).\n");                         /* Error */
            }                                                                                    /* End check */
        }                                                                                        /* End starve */
        else if (!strncmp(line, "node1 inject deadline ", 22) ||                                 /* Deadline */
                 !strncmp(line, "inject deadline ", 16)) {                                       /* Short */
            const char *p = strstr(line, "deadline ") + 9;                                       /* Params */
            int tid = 0, ms = 0;                                                                 /* Vars */
            if (sscanf(p, "%d %d", &tid, &ms) == 2 && tid >= 1 && tid <= MAX_TASKS) {            /* Scan */
                queue_node1_command(CMD_INJECT_DEADLINE, (uint32_t)tid, (uint32_t)ms);           /* Queue cmd */
            } else {                                                                             /* Error */
                printf("Usage: node1 inject deadline <1-3> <delay_ms>\n");                       /* Usage */
            }                                                                                    /* End check */
        }                                                                                        /* End deadline */
        else if (!strncmp(line, "node1 inject cpu ", 17) ||                                      /* CPU */
                 !strncmp(line, "inject cpu ", 11)) {                                            /* Short */
            const char *p = strstr(line, "cpu ") + 4;                                            /* State */
            bool on = (!strcmp(p, "on") || !strcmp(p, "1"));                                     /* Boolean */
            queue_node1_command(CMD_INJECT_CPU, 0, on ? 1 : 0);                                  /* Queue cmd */
        }                                                                                        /* End CPU */
        else if (!strncmp(line, "node1 inject ipc ", 17) ||                                      /* IPC */
                 !strncmp(line, "inject ipc ", 11)) {                                            /* Short */
            const char *p = strstr(line, "ipc ") + 4;                                            /* Delay */
            int ms = atoi(p);                                                                    /* Convert */
            queue_node1_command(CMD_INJECT_IPC, 0, (uint32_t)ms);                                /* Queue cmd */
        }                                                                                        /* End IPC */
        else if (!strcmp(line, "exit") || !strcmp(line, "quit")) {                               /* Exit */
            g.running = false;                                                                   /* Stop */
            break;                                                                               /* Break loop */
        }                                                                                        /* End exit */
        else {                                                                                   /* Unrecognized */
            printf("Unknown command '%s'. Type 'help' for command list.\n", line);               /* Prompt */
        }                                                                                        /* End dispatch */
    }                                                 /* End while */
    printf("\033[r\033[2J\033[1;1H");                 /* Reset scroll region and clear on exit */
    fflush(stdout);                                   /* Flush output buffer */
    return NULL;                                      /* Exit thread routine */
}                                                     /* Return from cli_thread */

int main(int argc, char *argv[]) {                    /* Supervisor node main function */
    signal(SIGPIPE, SIG_IGN);                         /* Ignore SIGPIPE on broken sockets */
    memset(&g, 0, sizeof(g));                         /* Zero initialize context */
    g.running       = true;                           /* Set active execution flag */
    g.node_id       = 2;                              /* Node identifier 2 */
    g.led_state     = LED_GREEN;                      /* Initial green healthy state */
    g.tcp_port      = TELEMETRY_PORT;                 /* Default telemetry TCP port */
    g.cpu_min       = 100.0f;                         /* Initial min CPU value */
    g.local_cpu_pct = 2.0f;                           /* Initial local CPU percentage */
    bool enable_gpio = true;                          /* Enable simulated GPIO by default */
    for (int i = 1; i < argc; i++) {                  /* Parse command line arguments */
        if (!strncmp(argv[i], "--port=", 7)) {        /* Check for port override */
            g.tcp_port = atoi(argv[i] + 7);           /* Store specified TCP port */
        }                                             /* End check */
        if (!strcmp(argv[i], "--no-gpio")) {          /* Check for GPIO disable flag */
            enable_gpio = false;                      /* Disable GPIO initialization */
        }                                             /* End check */
    }                                                 /* End argument loop */
    RT_MUTEX_INIT(&g.fault_mutex);                    /* Initialize fault registry mutex */
    RT_MUTEX_INIT(&g.analytics_mutex);                /* Initialize analytics mutex */
    RT_MUTEX_INIT(&g.cmd_mutex);                      /* Initialize command queue mutex */
    g.remote_tasks[0] = (TaskControlBlock){ .task_id = 1, .period_ms = 100, .deadline_ms = 80,  .state = TASK_STATE_RUNNING }; /* Task 1 */
    strncpy(g.remote_tasks[0].name, "Service_A (Traffic)", 31);                                  /* Task 1 name */
    g.remote_tasks[1] = (TaskControlBlock){ .task_id = 2, .period_ms = 200, .deadline_ms = 150, .state = TASK_STATE_RUNNING }; /* Task 2 */
    strncpy(g.remote_tasks[1].name, "Service_B (Grid)", 31);                                     /* Task 2 name */
    g.remote_tasks[2] = (TaskControlBlock){ .task_id = 3, .period_ms = 500, .deadline_ms = 400, .state = TASK_STATE_RUNNING }; /* Task 3 */
    strncpy(g.remote_tasks[2].name, "Service_C (Env)", 31);                                      /* Task 3 name */
    if (enable_gpio) hal_gpio_init();                 /* Initialize simulated GPIO subsystem */
    rt_thread_t th_tcp, th_mon, th_cli, th_gpio;      /* Thread identifiers */
    rt_thread_create(&th_mon, PRIORITY_MONITOR, supervisor_monitor_thread, NULL);   /* Start monitor thread */
    rt_thread_create(&th_tcp, PRIORITY_TELEMETRY, telemetry_server_thread, NULL);   /* Start TCP telemetry server */
    if (enable_gpio) {                                /* If GPIO is active */
        rt_thread_create(&th_gpio, PRIORITY_GPIO, hal_gpio_poll_thread, NULL);       /* Start GPIO poller */
    }                                                 /* End GPIO check */
    rt_thread_create(&th_cli, PRIORITY_CLI, cli_thread, NULL);                       /* Start interactive CLI thread */
    rt_thread_join(th_cli);                           /* Wait for CLI thread to terminate on exit command */
    g.running = false;                                /* Signal all threads to terminate */
    printf("\033[r");                                 /* Reset scroll region */
    fflush(stdout);                                   /* Flush buffer */
    sleep_ms(300);                                    /* Allow background threads time to exit cleanly */
    if (enable_gpio) hal_gpio_deinit();               /* Deinitialize GPIO subsystem */
    return 0;                                         /* Return success code */
}                                                     /* End of main function */
