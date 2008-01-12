#define _ATFILE_SOURCE

#include <sys/types.h>
#include <sys/syscall.h>

#define __OPENAT_MODE mode_t mode
#include "openat.h"

#ifdef __NEED_OPENAT

#ifdef __linux__

int openat(int dfd, const char * filename, int flags, __OPENAT_MODE)
{
	return syscall(SYS_openat, dfd, filename, flags, mode);
}

int mkdirat(int dfd, const char * pathname, mode_t mode)
{
	return syscall(SYS_mkdirat, dfd, pathname, mode);
}

int mknodat(int dfd, const char * filename, mode_t mode, dev_t dev)
{
	return syscall(SYS_mknodat, dfd, filename, mode, dev);
}

int fchownat(int dfd, const char * filename, uid_t owner, gid_t group, int flag)
{
	return syscall(SYS_fchownat, dfd, filename, owner, group, flag);
}

int futimesat(int dfd, const char * filename, const struct timeval * utimes)
{
	return syscall(SYS_futimesat, dfd, filename, utimes);
}

int fstatat64(int dfd, const char * path, struct stat64 * statbuf, int flag)
{
	return syscall(SYS_fstat64, dfd, path, statbuf, flag);
}

int unlinkat(int dfd, const char * pathname, int flag)
{
	return syscall(SYS_unlinkat, dfd, pathname, flag);
}

int renameat(int olddfd, const char * oldname, int newdfd, const char * newname)
{
	return syscall(SYS_renameat, olddfd, oldname, newdfd, newname);
}

int linkat(int olddfd, const char * oldname, int newdfd, const char * newname, int flags)
{
	return syscall(SYS_linkat, olddfd, oldname, newdfd, newname, flags);
}

int symlinkat(const char * oldname, int newdfd, const char * newname)
{
	return syscall(SYS_symlinkat, oldname, newdfd, newname);
}

int readlinkat(int dfd, const char * path, char * buf, int bufsiz)
{
	return syscall(SYS_readlinkat, dfd, path, buf, bufsiz);
}

int fchmodat(int dfd, const char * filename, mode_t mode)
{
	return syscall(SYS_fchmodat, dfd, filename, mode);
}

int faccessat(int dfd, const char * filename, int mode)
{
	return syscall(SYS_faccessat, dfd, filename, mode);
}

#else /* __linux__ */

/* non-thread-safe simulated versions */

#error Finish this section

int openat(int dfd, const char * filename, int flags, __OPENAT_MODE)
{
}

int mkdirat(int dfd, const char * pathname, mode_t mode)
{
}

int mknodat(int dfd, const char * filename, mode_t mode, dev_t dev)
{
}

int fchownat(int dfd, const char * filename, uid_t owner, gid_t group, int flag)
{
}

int futimesat(int dfd, const char * filename, const struct timeval * utimes)
{
}

int fstatat64(int dfd, const char * path, struct stat64 * statbuf, int flag)
{
}

int unlinkat(int dfd, const char * pathname, int flag)
{
}

int renameat(int olddfd, const char * oldname, int newdfd, const char * newname)
{
}

int linkat(int olddfd, const char * oldname, int newdfd, const char * newname, int flags)
{
}

int symlinkat(const char * oldname, int newdfd, const char * newname)
{
}

int readlinkat(int dfd, const char * path, char * buf, int bufsiz)
{
}

int fchmodat(int dfd, const char * filename, mode_t mode)
{
}

int faccessat(int dfd, const char * filename, int mode)
{
}

#endif /* __linux__ */

#endif /* __NEED_OPENAT */
