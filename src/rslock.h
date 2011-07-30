/**************************************************************************
 * rslock.h
 *
 * Modified: 2011.211
 **************************************************************************/

#ifndef RSLOCK_H
#define RSLOCK_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

/* Control the use of spinlocks for rslocks, otherwise mutexes are used */
#define RSLOCK_USE_SPINLOCK 1

/* Map spinlock type for Apple Mac OSX */
#if defined(__APPLE__)
#include <libkern/OSAtomic.h>
typedef OSSpinLock pthread_spinlock_t;
#endif /* __APPLE__ */

/* Define rslock_t as needed for lock type */
#if defined(RSLOCK_USE_SPINLOCK)
typedef pthread_spinlock_t rslock_t;
#else
typedef pthread_mutex_t rslock_t;
#endif /* RSLOCK_USE_SPINLOCK */

extern inline int rslock_init (rslock_t *lock);
extern inline int rslock_destroy (rslock_t *lock);
extern inline int rslock_lock (rslock_t *lock);
extern inline int rslock_unlock (rslock_t *lock);

#ifdef __cplusplus
}
#endif

#endif /* RSLOCK_H */
