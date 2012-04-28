/**************************************************************************
 * rslock.c
 *
 * Modified: 2012.120
 **************************************************************************/

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
