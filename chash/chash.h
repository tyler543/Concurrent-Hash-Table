#ifndef CHASH_H
#define CHASH_H

#include <stdint.h>

typedef struct hash_struct {
    uint32_t hash;
    char name[50];
    uint32_t salary;
    struct hash_struct *next;
} hashRecord;

typedef enum {
    CMD_INSERT,
    CMD_DELETE,
    CMD_UPDATE,
    CMD_SEARCH,
    CMD_PRINT,
    CMD_INVALID
} command_type;

typedef struct {
    command_type type;
    char name[50];
    uint32_t salary;   /* for insert/update */
    int priority;      /* priority number */
    int seq;           /* FIFO sequence among same-priority commands */
    int original_index;/* order in file (optional) */
} command_t;

/* utilities */
uint32_t jenkins_one_at_a_time_hash(const char *key);
long long current_timestamp_us(void);
void log_message(const char *fmt, ...);

#endif /* CHASH_H */
