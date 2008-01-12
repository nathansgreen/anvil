#define _ATFILE_SOURCE

#include <unistd.h>
#include <sys/stat.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/syscall.h>

/* can be removed once ENOSYS, perror() are not used */
#include <errno.h>
#include <stdio.h>

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

int futimesat(int dfd, const char * filename, const struct timeval * times)
{
	return syscall(SYS_futimesat, dfd, filename, times);
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

//#error Finish this section

int openat(int dfd, const char * filename, int flags, __OPENAT_MODE)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *filename == '/')
		return open(filename, flags, mode);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = open(filename, flags, mode);
	fchdir(cfd);
	close(cfd);
	return r;
}

int mkdirat(int dfd, const char * pathname, mode_t mode)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *pathname == '/')
		return mkdir(pathname, mode);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = mkdir(pathname, mode);
	fchdir(cfd);
	close(cfd);
	return r;
}

int mknodat(int dfd, const char * filename, mode_t mode, dev_t dev)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *filename == '/')
		return mknod(filename, mode, dev);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = mknod(filename, mode, dev);
	fchdir(cfd);
	close(cfd);
	return r;
}

int fchownat(int dfd, const char * filename, uid_t owner, gid_t group, int flag)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *filename == '/')
	{
		if(flag & AT_SYMLINK_NOFOLLOW)
			return lchown(filename, owner, group);
		return chown(filename, owner, group);
	}
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	if(flag & AT_SYMLINK_NOFOLLOW)
		r = lchown(filename, owner, group);
	else
		r = chown(filename, owner, group);
	fchdir(cfd);
	close(cfd);
	return r;
}

int futimesat(int dfd, const char * filename, const struct timeval * times)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *filename == '/')
		return utimes(filename, times);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = utimes(filename, times);
	fchdir(cfd);
	close(cfd);
	return r;
}

int fstatat64(int dfd, const char * path, struct stat64 * statbuf, int flag)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *path == '/')
	{
		if(flag & AT_SYMLINK_NOFOLLOW)
			return lstat64(path, statbuf);
		return stat64(path, statbuf);
	}
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	if(flag & AT_SYMLINK_NOFOLLOW)
		r = lstat64(path, statbuf);
	else
		r = stat64(path, statbuf);
	fchdir(cfd);
	close(cfd);
	return r;
}

int unlinkat(int dfd, const char * pathname, int flag)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *pathname == '/')
	{
		if(flag & AT_REMOVEDIR)
			return rmdir(pathname);
		return unlink(pathname);
	}
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	if(flag & AT_REMOVEDIR)
		r = rmdir(pathname);
	else
		r = unlink(pathname);
	fchdir(cfd);
	close(cfd);
	return r;
}

int renameat(int olddfd, const char * oldname, int newdfd, const char * newname)
{
	/* How to emulate this? */
	/* really only a problem if olddfd != newdfd and both != AT_FDCWD */
	errno = ENOSYS;
	perror(__FUNCTION__);
	return -errno;
}

int linkat(int olddfd, const char * oldname, int newdfd, const char * newname, int flags)
{
	/* How to emulate this? */
	/* really only a problem if olddfd != newdfd and both != AT_FDCWD */
	errno = ENOSYS;
	perror(__FUNCTION__);
	return -errno;
}

int symlinkat(const char * oldname, int newdfd, const char * newname)
{
	int cfd, r;
	if(newdfd == AT_FDCWD || *newname == '/')
		return symlink(oldname, newname);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(newdfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = symlink(oldname, newname);
	fchdir(cfd);
	close(cfd);
	return r;
}

int readlinkat(int dfd, const char * path, char * buf, int bufsiz)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *path == '/')
		return readlink(path, buf, bufsiz);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = readlink(path, buf, bufsiz);
	fchdir(cfd);
	close(cfd);
	return r;
}

int fchmodat(int dfd, const char * filename, mode_t mode)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *filename == '/')
		return chmod(filename, mode);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = chmod(filename, mode);
	fchdir(cfd);
	close(cfd);
	return r;
}

int faccessat(int dfd, const char * filename, int mode)
{
	int cfd, r;
	if(dfd == AT_FDCWD || *filename == '/')
		return access(filename, mode);
	cfd = open(".", 0);
	if(cfd < 0)
		return cfd;
	r = fchdir(dfd);
	if(r < 0)
	{
		close(cfd);
		return r;
	}
	r = access(filename, mode);
	fchdir(cfd);
	close(cfd);
	return r;
}

#endif /* __linux__ */

#endif /* __NEED_OPENAT */
