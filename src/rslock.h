/**************************************************************************
 * rslock.h
 *
 * Modified: 2012.120
 **************************************************************************/

#ifndef RSLOCK_H
#define RSLOCK_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

/* Map spinlock type for Apple Mac OSX */
#ifdef __APPLE__

#include <libkern/OSAtomic.h>
typedef OSSpinLock pthread_spinlock_t;

int pthread_spin_init(pthread_spinlock_t* lock, int pshared);
int pthread_spin_destroy(pthread_spinlock_t* lock);
int pthread_spin_lock(pthread_spinlock_t* lock);
int pthread_spin_unlock(pthread_spinlock_t* lock);

#endif /* __APPLE__ */

#ifdef __cplusplus
}
#endif

#endif /* RSLOCK_H */
