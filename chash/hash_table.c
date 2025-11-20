#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "hash_table.h"
#include "chash.h"

/* singly-linked list head (sorted by hash) */
static hashRecord *head = NULL;
/* rwlock protecting the list */
static pthread_rwlock_t list_rwlock;

/* initialize/destroy */
void ht_init(void) {
    head = NULL;
    pthread_rwlock_init(&list_rwlock, NULL);
}
void ht_destroy(void) {
    pthread_rwlock_wrlock(&list_rwlock);
    hashRecord *cur = head;
    while (cur) {
        hashRecord *tmp = cur;
        cur = cur->next;
        free(tmp);
    }
    head = NULL;
    pthread_rwlock_unlock(&list_rwlock);
    pthread_rwlock_destroy(&list_rwlock);
}

/* helper: find prev by hash */
static hashRecord *find_prev_by_hash(uint32_t hash, hashRecord **prev_out) {
    hashRecord *prev = NULL;
    hashRecord *cur = head;
    while (cur && cur->hash < hash) {
        prev = cur;
        cur = cur->next;
    }
    if (prev_out) *prev_out = prev;
    return cur;
}

/* Insert */
int ht_insert(const char *name, uint32_t salary, uint32_t hash_out, int thread_prio) {
    log_message("%lld: THREAD %d WRITE LOCK ACQUIRE ATTEMPT", current_timestamp_us(), thread_prio);
    pthread_rwlock_wrlock(&list_rwlock);
    log_message("%lld: THREAD %d WRITE LOCK ACQUIRED", current_timestamp_us(), thread_prio);

    hashRecord *prev = NULL;
    hashRecord *cur = find_prev_by_hash(hash_out, &prev);
    if (cur && cur->hash == hash_out) {
        log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
        pthread_rwlock_unlock(&list_rwlock);
        return -1;
    }
    hashRecord *node = malloc(sizeof(hashRecord));
    if (!node) {
        log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
        pthread_rwlock_unlock(&list_rwlock);
        return -1;
    }
    node->hash = hash_out;
    strncpy(node->name, name, sizeof(node->name)-1);
    node->name[sizeof(node->name)-1] = '\0';
    node->salary = salary;

    if (!prev) {
        node->next = head;
        head = node;
    } else {
        node->next = prev->next;
        prev->next = node;
    }

    log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
    pthread_rwlock_unlock(&list_rwlock);
    return 0;
}

/* Delete: on success, out_deleted_salary filled if non-NULL */
int ht_delete(const char *name, uint32_t hash_out, int thread_prio, uint32_t *out_deleted_salary) {
    log_message("%lld: THREAD %d WRITE LOCK ACQUIRE ATTEMPT", current_timestamp_us(), thread_prio);
    pthread_rwlock_wrlock(&list_rwlock);
    log_message("%lld: THREAD %d WRITE LOCK ACQUIRED", current_timestamp_us(), thread_prio);

    hashRecord *prev = NULL;
    hashRecord *cur = head;
    while (cur && cur->hash < hash_out) {
        prev = cur;
        cur = cur->next;
    }
    if (!cur || cur->hash != hash_out) {
        log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
        pthread_rwlock_unlock(&list_rwlock);
        return -1;
    }
    /* found */
    if (out_deleted_salary) *out_deleted_salary = cur->salary;
    if (!prev) head = cur->next;
    else prev->next = cur->next;
    free(cur);

    log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
    pthread_rwlock_unlock(&list_rwlock);
    return 0;
}

/* Update: return old salary via out_old_salary if non-NULL */
int ht_update(const char *name, uint32_t new_salary, uint32_t hash_out, int thread_prio, uint32_t *out_old_salary) {
    log_message("%lld: THREAD %d WRITE LOCK ACQUIRE ATTEMPT", current_timestamp_us(), thread_prio);
    pthread_rwlock_wrlock(&list_rwlock);
    log_message("%lld: THREAD %d WRITE LOCK ACQUIRED", current_timestamp_us(), thread_prio);

    hashRecord *cur = head;
    while (cur && cur->hash < hash_out) cur = cur->next;
    if (!cur || cur->hash != hash_out) {
        log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
        pthread_rwlock_unlock(&list_rwlock);
        return -1;
    }
    if (out_old_salary) *out_old_salary = cur->salary;
    cur->salary = new_salary;

    log_message("%lld: THREAD %d WRITE LOCK RELEASED", current_timestamp_us(), thread_prio);
    pthread_rwlock_unlock(&list_rwlock);
    return 0;
}

/* Search: returns malloc'd copy of record or NULL */
hashRecord *ht_search(const char *name, uint32_t hash_out, int thread_prio) {
    log_message("%lld: THREAD %d READ LOCK ACQUIRE ATTEMPT", current_timestamp_us(), thread_prio);
    pthread_rwlock_rdlock(&list_rwlock);
    log_message("%lld: THREAD %d READ LOCK ACQUIRED", current_timestamp_us(), thread_prio);

    hashRecord *cur = head;
    while (cur && cur->hash < hash_out) cur = cur->next;
    hashRecord *res = NULL;
    if (cur && cur->hash == hash_out) {
        res = malloc(sizeof(hashRecord));
        if (res) memcpy(res, cur, sizeof(hashRecord));
    }

    log_message("%lld: THREAD %d READ LOCK RELEASED", current_timestamp_us(), thread_prio);
    pthread_rwlock_unlock(&list_rwlock);
    return res;
}

/* Print all records (sorted by hash) to stdout */
void ht_print_all(int thread_prio) {
    log_message("%lld: THREAD %d READ LOCK ACQUIRE ATTEMPT", current_timestamp_us(), thread_prio);
    pthread_rwlock_rdlock(&list_rwlock);
    log_message("%lld: THREAD %d READ LOCK ACQUIRED", current_timestamp_us(), thread_prio);

    printf("Current Database:\n");
    hashRecord *cur = head;
    while (cur) {
        printf("%u,%s,%u\n", cur->hash, cur->name, cur->salary);
        cur = cur->next;
    }

    log_message("%lld: THREAD %d READ LOCK RELEASED", current_timestamp_us(), thread_prio);
    pthread_rwlock_unlock(&list_rwlock);
}
