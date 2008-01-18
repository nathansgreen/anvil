/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE
#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <dirent.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include "openat.h"

FILE * fopenat(int dfd, const char * filename, const char * mode)
{
	int fd, p = mode[0] ? ((mode[1] == 'b') ? 2 : 1) : 0;
	mode_t modet;
	FILE * file;
	if(mode[0] == 'r')
		modet = (mode[p] == '+') ? O_RDWR : O_RDONLY;
	else if(mode[0] == 'w')
		modet = O_CREAT | O_TRUNC | ((mode[p] == '+') ? O_RDWR : O_WRONLY);
	else if(mode[0] == 'a')
		modet = O_CREAT | O_APPEND | ((mode[p] == '+') ? O_RDWR : O_WRONLY);
	else
	{
		errno = EINVAL;
		return NULL;
	}
	fd = openat(dfd, filename, modet, S_IRWXU | S_IRWXG | S_IRWXO);
	if(fd < 0)
		return NULL;
	file = fdopen(fd, mode);
	if(!file)
	{
		int save = errno;
		close(fd);
		errno = save;
	}
	return file;
}

DIR * opendirat(int dfd, const char * pathname)
{
	DIR * dir;
	if(dfd == AT_FDCWD || *pathname == '/')
		return opendir(pathname);
	dfd = openat(dfd, pathname, O_RDONLY);
	if(dfd < 0)
		return NULL;
	dir = fdopendir(dfd);
	if(!dir)
	{
		int save = errno;
		close(dfd);
		errno = save;
	}
	return dir;
}

DIR * fdopendir(int dfd)
{
	static DIR * (*libc_fdopendir)(int) = NULL;
	static int libc_checked = 0;
	int save, cwd;
	DIR * dir;
	if(!libc_checked)
	{
		libc_fdopendir = dlsym(RTLD_NEXT, __FUNCTION__);
		libc_checked = 1;
	}
	if(libc_fdopendir)
		return libc_fdopendir(dfd);
	cwd = open(".", 0);
	if(cwd < 0)
		return NULL;
	if(fchdir(dfd) < 0)
	{
		save = errno;
		close(cwd);
		errno = save;
		return NULL;
	}
	dir = opendir(".");
	save = errno;
	fchdir(cwd);
	close(cwd);
	errno = save;
	return dir;
}

#ifdef __NEED_OPENAT

#ifdef __linux__

int openat(int dfd, const char * filename, int flags, ...)
{
	va_list ap;
	mode_t mode;
	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);
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

int openat(int dfd, const char * filename, int flags, ...)
{
	int cfd, r;
	va_list ap;
	mode_t mode;
	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);
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
