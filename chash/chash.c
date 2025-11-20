#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/time.h>
#include "chash.h"
#include "hash_table.h"

/* logging file and mutex */
static FILE *log_fp = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

/* scheduling state */
/* active_priority = the priority currently being executed (starts at min found) */
static pthread_mutex_t sched_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sched_cv = PTHREAD_COND_INITIALIZER;
static int active_priority = -1;

/* For FIFO within a priority, maintain next_seq_to_run[priority] and counts */
static int *next_seq_to_run = NULL;     /* next sequence number to run for priority p */
static int *count_for_prio = NULL;      /* number of commands with priority p */
static int max_priority = -1;

/* parsed commands array (one entry per command) */
static command_t *commands = NULL;
static int num_commands = 0;

/* Utility: write to hash.log with microsecond timestamp messages already provided by caller */
void log_message(const char *fmt, ...) {
    pthread_mutex_lock(&log_mutex);
    if (!log_fp) {
        log_fp = fopen("hash.log", "w");
        if (!log_fp) {
            perror("log open");
            pthread_mutex_unlock(&log_mutex);
            return;
        }
    }
    va_list ap;
    va_start(ap, fmt);
    vfprintf(log_fp, fmt, ap);
    fprintf(log_fp, "\n");
    fflush(log_fp);
    va_end(ap);
    pthread_mutex_unlock(&log_mutex);
}

long long current_timestamp_us(void) {
    struct timeval te;
    gettimeofday(&te, NULL);
    return (long long)te.tv_sec * 1000000LL + te.tv_usec;
}

/* Jenkins one-at-a-time hash */
uint32_t jenkins_one_at_a_time_hash(const char *key) {
    uint32_t hash = 0;
    while (*key) {
        hash += (unsigned char)(*key++);
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

/* trim helper */
static char *trim(char *s) {
    if (!s) return s;
    while (*s == ' ' || *s == '\t') s++;
    char *end = s + strlen(s) - 1;
    while (end >= s && (*end == '\n' || *end == '\r' || *end == ' ' || *end == '\t')) { *end = '\0'; end--; }
    return s;
}

/* Parse a single CSV line into command_t; returns 0 on success, -1 on failure, 1 if header 'threads' processed */
static int parse_line_to_command(const char *line, command_t *out) {
    char *copy = strdup(line);
    if (!copy) return -1;
    char *tokens[16];
    int t = 0;
    char *p = copy;
    while (t < 16) {
        char *tok = strsep(&p, ",");
        if (!tok) break;
        tokens[t++] = trim(tok);
    }
    if (t == 0) { free(copy); return -1; }

    if (strcasecmp(tokens[0], "threads") == 0) {
        /* header: threads,<N>,... we'll just ignore the explicit N in parsing because we derive counts */
        free(copy);
        return 1;
    }

    /* last token is priority */
    int priority = atoi(tokens[t-1]);

    /* initialize */
    out->priority = priority;
    out->seq = -1;
    out->salary = 0;
    out->original_index = -1;
    out->type = CMD_INVALID;
    out->name[0] = '\0';

    if (strcasecmp(tokens[0], "insert") == 0) {
        if (t < 4) { free(copy); return -1; }
        strncpy(out->name, tokens[1], sizeof(out->name)-1);
        out->salary = (uint32_t)atoi(tokens[t-2]); /* second-last is salary */
        out->type = CMD_INSERT;
    } else if (strcasecmp(tokens[0], "delete") == 0) {
        if (t < 3) { free(copy); return -1; }
        strncpy(out->name, tokens[1], sizeof(out->name)-1);
        out->type = CMD_DELETE;
    } else if (strcasecmp(tokens[0], "update") == 0) {
        /* formats vary; typically: update,Name,newSalary,priority */
        if (t < 4) { free(copy); return -1; }
        strncpy(out->name, tokens[1], sizeof(out->name)-1);
        out->salary = (uint32_t)atoi(tokens[t-2]);
        out->type = CMD_UPDATE;
    } else if (strcasecmp(tokens[0], "search") == 0) {
        if (t < 3) { free(copy); return -1; }
        strncpy(out->name, tokens[1], sizeof(out->name)-1);
        out->type = CMD_SEARCH;
    } else if (strcasecmp(tokens[0], "print") == 0) {
        out->type = CMD_PRINT;
    } else {
        free(copy);
        return -1;
    }

    free(copy);
    return 0;
}

/* Worker thread: one per command */
static void *worker(void *arg) {
    command_t cmd = *(command_t*)arg;
    /* Log WAITING */
    log_message("%lld: THREAD %d WAITING FOR MY TURN", current_timestamp_us(), cmd.priority);

    /* Wait until active_priority == cmd.priority and seq matches FIFO token */
    pthread_mutex_lock(&sched_mutex);
    while (active_priority != cmd.priority || next_seq_to_run[cmd.priority] != cmd.seq) {
        pthread_cond_wait(&sched_cv, &sched_mutex);
    }
    /* now it's this command's turn */
    log_message("%lld: THREAD %d AWAKENED FOR WORK", current_timestamp_us(), cmd.priority);
    pthread_mutex_unlock(&sched_mutex);

    /* Execute command and write proper logs and console output */
    if (cmd.type == CMD_INSERT) {
        uint32_t h = jenkins_one_at_a_time_hash(cmd.name);
        log_message("%lld: THREAD %d INSERT,%u,%s,%u", current_timestamp_us(), cmd.priority, h, cmd.name, cmd.salary);
        int rc = ht_insert(cmd.name, cmd.salary, h, cmd.priority);
        if (rc == 0) {
            printf("Inserted %u,%s,%u\n", h, cmd.name, cmd.salary);
        } else {
            printf("Insert failed. Entry %u is a duplicate.\n", h);
        }
    } else if (cmd.type == CMD_DELETE) {
        uint32_t h = jenkins_one_at_a_time_hash(cmd.name);
        log_message("%lld: THREAD %d DELETE,%u,%s", current_timestamp_us(), cmd.priority, h, cmd.name);
        uint32_t deleted_salary = 0;
        int rc = ht_delete(cmd.name, h, cmd.priority, &deleted_salary);
        if (rc == 0) {
            printf("Deleted record for %u,%s,%u\n", h, cmd.name, deleted_salary);
        } else {
            printf("%s not found.\n", cmd.name);
        }
    } else if (cmd.type == CMD_UPDATE) {
        uint32_t h = jenkins_one_at_a_time_hash(cmd.name);
        log_message("%lld: THREAD %d UPDATE,%u,%s,%u", current_timestamp_us(), cmd.priority, h, cmd.name, cmd.salary);
        uint32_t old_salary = 0;
        int rc = ht_update(cmd.name, cmd.salary, h, cmd.priority, &old_salary);
        if (rc == 0) {
            printf("Updated record %u from %u,%s,%u to %u,%s,%u\n",
                   h, h, cmd.name, old_salary, h, cmd.name, cmd.salary);
        } else {
            printf("Update failed. Entry %u not found.\n", h);
        }
    } else if (cmd.type == CMD_SEARCH) {
        uint32_t h = jenkins_one_at_a_time_hash(cmd.name);
        log_message("%lld: THREAD %d SEARCH,%u,%s", current_timestamp_us(), cmd.priority, h, cmd.name);
        hashRecord *rec = ht_search(cmd.name, h, cmd.priority);
        if (rec) {
            printf("Found: %u,%s,%u\n", rec->hash, rec->name, rec->salary);
            free(rec);
        } else {
            printf("%s not found.\n", cmd.name);
        }
    } else if (cmd.type == CMD_PRINT) {
        log_message("%lld: THREAD %d PRINT", current_timestamp_us(), cmd.priority);
        ht_print_all(cmd.priority);
    }

    /* Mark completion for this priority seq, possibly advance active_priority */
    pthread_mutex_lock(&sched_mutex);
    next_seq_to_run[cmd.priority]++; /* allow next FIFO item at same priority */
    /* If that exhausted this priority, advance active_priority to next priority that has commands */
    if (next_seq_to_run[cmd.priority] >= count_for_prio[cmd.priority]) {
        /* find next priority > current that has count > 0 */
        int p;
        for (p = cmd.priority + 1; p <= max_priority; ++p) {
            if (count_for_prio[p] > 0) break;
        }
        if (p <= max_priority) active_priority = p;
        else active_priority = -1; /* finished */
    }
    pthread_cond_broadcast(&sched_cv);
    pthread_mutex_unlock(&sched_mutex);

    return NULL;
}

int main(int argc, char **argv) {
    /* clear log at start */
    pthread_mutex_lock(&log_mutex);
    if (log_fp) fclose(log_fp);
    log_fp = fopen("hash.log", "w");
    if (!log_fp) {
        perror("Unable to open hash.log");
        pthread_mutex_unlock(&log_mutex);
        return 1;
    }
    fclose(log_fp);
    log_fp = NULL;
    pthread_mutex_unlock(&log_mutex);

    ht_init();

    /* Read commands.txt */
    FILE *f = fopen("commands.txt", "r");
    if (!f) {
        perror("Unable to open commands.txt in working directory");
        ht_destroy();
        return 1;
    }

    /* Read lines into temporary array */
    char *line = NULL;
    size_t len = 0;
    ssize_t r;
    command_t *tmp_commands = NULL;
    int tmp_cap = 0;
    int tmp_n = 0;

    while ((r = getline(&line, &len, f)) != -1) {
        char *t = trim(line);
        if (t[0] == '\0') continue;
        command_t cmd;
        int parse_ret = parse_line_to_command(t, &cmd);
        if (parse_ret == 1) {
            /* header 'threads' - ignore as we derive priorities from commands themselves */
            continue;
        } else if (parse_ret == 0) {
            /* push */
            if (tmp_n + 1 > tmp_cap) {
                tmp_cap = tmp_cap ? tmp_cap * 2 : 64;
                tmp_commands = realloc(tmp_commands, sizeof(command_t) * tmp_cap);
                if (!tmp_commands) { perror("realloc"); free(line); fclose(f); ht_destroy(); return 1; }
            }
            cmd.original_index = tmp_n;
            tmp_commands[tmp_n++] = cmd;
        } else {
            fprintf(stderr, "Warning: skipping unparsable line: %s\n", t);
        }
    }
    free(line);
    fclose(f);

    if (tmp_n == 0) {
        fprintf(stderr, "No commands found in commands.txt\n");
        free(tmp_commands);
        ht_destroy();
        return 0;
    }

    /* Determine max priority and count per priority, and assign seq numbers (FIFO order) */
    max_priority = -1;
    for (int i = 0; i < tmp_n; ++i) if (tmp_commands[i].priority > max_priority) max_priority = tmp_commands[i].priority;
    if (max_priority < 0) max_priority = 0;

    /* allocate counters sized max_priority+1 */
    next_seq_to_run = calloc(max_priority + 1, sizeof(int));
    count_for_prio = calloc(max_priority + 1, sizeof(int));
    if (!next_seq_to_run || !count_for_prio) { perror("calloc"); free(tmp_commands); ht_destroy(); return 1; }

    /* First pass to count */
    for (int i = 0; i < tmp_n; ++i) {
        int p = tmp_commands[i].priority;
        count_for_prio[p]++;
    }

    /* assign seq numbers per priority by iterating file order */
    int *seq_alloc = calloc(max_priority + 1, sizeof(int));
    if (!seq_alloc) { perror("calloc2"); free(tmp_commands); ht_destroy(); return 1; }
    for (int i = 0; i < tmp_n; ++i) {
        int p = tmp_commands[i].priority;
        tmp_commands[i].seq = seq_alloc[p]++;
    }
    free(seq_alloc);

    /* Build commands array (copy) */
    commands = malloc(sizeof(command_t) * tmp_n);
    if (!commands) { perror("malloc"); free(tmp_commands); ht_destroy(); return 1; }
    num_commands = tmp_n;
    for (int i = 0; i < num_commands; ++i) commands[i] = tmp_commands[i];
    free(tmp_commands);

    /* Set initial active_priority to the smallest priority that has commands (FIFO across priorities ascending) */
    int start_p = -1;
    for (int p = 0; p <= max_priority; ++p) if (count_for_prio[p] > 0) { start_p = p; break; }
    if (start_p == -1) start_p = 0;
    active_priority = start_p;

    /* spawn one thread per command */
    pthread_t *tids = malloc(sizeof(pthread_t) * num_commands);
    if (!tids) { perror("malloc"); ht_destroy(); return 1; }

    for (int i = 0; i < num_commands; ++i) {
        /* create thread; pass pointer to commands[i] */
        command_t *arg = &commands[i];
        if (pthread_create(&tids[i], NULL, worker, arg) != 0) {
            perror("pthread_create");
            tids[i] = 0;
        }
    }

    /* Broadcast to wake waiting threads so they check active_priority */
    pthread_mutex_lock(&sched_mutex);
    pthread_cond_broadcast(&sched_cv);
    pthread_mutex_unlock(&sched_mutex);

    /* join threads */
    for (int i = 0; i < num_commands; ++i) {
        if (tids[i]) pthread_join(tids[i], NULL);
    }

    /* final print as required */
    /* If active_priority != -1 it means some priorities remain but all threads have completed; ht_print_all will still print final state */
    ht_print_all(-1);

    /* cleanup */
    ht_destroy();
    free(tids);
    free(commands);
    free(next_seq_to_run);
    free(count_for_prio);
    if (log_fp) { fclose(log_fp); log_fp = NULL; }

    return 0;
}
