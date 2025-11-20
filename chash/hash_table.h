#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "chash.h"

/* Initialize and destroy table */
void ht_init(void);
void ht_destroy(void);

/* Thread-id aware operations (thread_prio used in logs) */
/* return values:
   insert: 0 success, -1 duplicate
   delete: 0 success, -1 not found (if success, out_deleted_salary is filled if non-NULL)
   update: 0 success, -1 not found (old salary returned via out_old_salary if non-NULL)
   search: returns malloc'd copy of record or NULL
*/
int ht_insert(const char *name, uint32_t salary, uint32_t hash_out, int thread_prio);
int ht_delete(const char *name, uint32_t hash_out, int thread_prio, uint32_t *out_deleted_salary);
int ht_update(const char *name, uint32_t new_salary, uint32_t hash_out, int thread_prio, uint32_t *out_old_salary);
hashRecord *ht_search(const char *name, uint32_t hash_out, int thread_prio);
void ht_print_all(int thread_prio);

#endif /* HASH_TABLE_H */
