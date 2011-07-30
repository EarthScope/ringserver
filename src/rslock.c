/**************************************************************************
 * rslock.c
 *
 * Modified: 2011.211
 **************************************************************************/

#include <pthread.h>
#include "rslock.h"

/* Map Apple Mac OSX spinlock to pthread_spin routines */
#if defined(__APPLE__)
inline int pthread_spin_init(pthread_spinlock_t* lock, int pshared) {
  *lock = 0;
  return 0;
}
inline int pthread_spin_destroy(pthread_spinlock_t* lock) {
  return 0;
}
inline int pthread_spin_lock(pthread_spinlock_t* lock) {
  OSSpinLockLock(lock);
  return 0;
}
inline int pthread_spin_unlock(pthread_spinlock_t* lock) {
  OSSpinLockUnlock(lock);
  return 0;
}
#endif /* __APPLE__ */


/***************************************************************************
 * rslock_init:
 *
 * Generalize ring access lock initializing.
 ***************************************************************************/
inline int
rslock_init (rslock_t *lock)
{
#ifdef RSLOCK_USE_SPINLOCK
  return pthread_spin_init ((pthread_spinlock_t *)lock, 0);
#else
  return pthread_mutex_init ((pthread_mutex_t *)lock, NULL);
#endif
}  /* End of rslock_init() */


/***************************************************************************
 * rslock_destroy:
 *
 * Generalize ring access lock destruction.
 ***************************************************************************/
inline int
rslock_destroy (rslock_t *lock)
{
#ifdef RSLOCK_USE_SPINLOCK
  return pthread_spin_destroy ((pthread_spinlock_t *)lock);
#else
  return pthread_mutex_destroy ((pthread_mutex_t *)lock);
#endif
}  /* End of rslock_destroy() */


/***************************************************************************
 * rslock_lock:
 *
 * Generalize ring access locking.
 ***************************************************************************/
inline int
rslock_lock (rslock_t *lock)
{
#ifdef RSLOCK_USE_SPINLOCK
  return pthread_spin_lock ((pthread_spinlock_t *)lock);
#else
  return pthread_mutex_lock ((pthread_mutex_t *)lock);
#endif
}  /* End of rslock_lock() */


/***************************************************************************
 * rslock_unlock:
 *
 * Generalize ring access unlocking.
 ***************************************************************************/
inline int
rslock_unlock (rslock_t *lock)
{
#ifdef RSLOCK_USE_SPINLOCK
  return pthread_spin_unlock ((pthread_spinlock_t *)lock);
#else
  return pthread_mutex_unlock ((pthread_mutex_t *)lock);
#endif
}  /* End of rslock_unlock() */
