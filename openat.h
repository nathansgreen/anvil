/* This file is part of Toilet. Toilet is copyright 2007-2008 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __OPENAT_H
#define __OPENAT_H

/* This macro enables the *at() calls normally, so without it we disable everything. */
#ifdef _ATFILE_SOURCE

#include <fcntl.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* a useful utility function */
FILE * fopenat(int dfd, const char * filename, const char * mode);

/* not part of the standard *at() functions */
DIR * opendirat(int dfd, const char * pathname);
DIR * fdopendir(int dfd);

#ifdef __cplusplus
}
#endif

#ifndef AT_FDCWD

#include <sys/types.h>
#include <sys/syscall.h>

/* assume that AT_FDCWD is a good indicator of whether we need anything at all */
#define __NEED_OPENAT

#define AT_FDCWD -100
#define AT_SYMLINK_NOFOLLOW 0x100
#define AT_REMOVEDIR 0x200
#define AT_SYMLINK_FOLLOW 0x400

#ifndef __OPENAT_MODE
#define __OPENAT_MODE ...
#endif

#if defined(__APPLE__) && defined(__MACH__)
/* these are actually available on 10.5, but how to tell that here? */
#define stat64 stat
#define lstat64 lstat
#endif

struct timeval;
struct stat64;

#ifdef __cplusplus
extern "C" {
#endif

int openat(int dfd, const char * filename, int flags, __OPENAT_MODE) __attribute__((weak));
int mkdirat(int dfd, const char * pathname, mode_t mode) __attribute__((weak));
int mknodat(int dfd, const char * filename, mode_t mode, dev_t dev) __attribute__((weak));
int fchownat(int dfd, const char * filename, uid_t owner, gid_t group, int flag) __attribute__((weak));
int futimesat(int dfd, const char * filename, const struct timeval * times) __attribute__((weak));
int fstatat64(int dfd, const char * path, struct stat64 * statbuf, int flag) __attribute__((weak));
int unlinkat(int dfd, const char * pathname, int flag) __attribute__((weak));
int renameat(int olddfd, const char * oldname, int newdfd, const char * newname) __attribute__((weak));
int linkat(int olddfd, const char * oldname, int newdfd, const char * newname, int flags) __attribute__((weak));
int symlinkat(const char * oldname, int newdfd, const char * newname) __attribute__((weak));
int readlinkat(int dfd, const char * path, char * buf, int bufsiz) __attribute__((weak));
int fchmodat(int dfd, const char * filename, mode_t mode) __attribute__((weak));
int faccessat(int dfd, const char * filename, int mode) __attribute__((weak));

#ifdef __cplusplus
}
#endif

#ifdef __linux__
#ifdef __i386__

#ifndef SYS_openat
#define SYS_openat 295
#define SYS_mkdirat 296
#define SYS_mknodat 297
#define SYS_fchownat 298
#define SYS_futimesat 299
#define SYS_fstatat64 300
#define SYS_unlinkat 301
#define SYS_renameat 302
#define SYS_linkat 303
#define SYS_symlinkat 304
#define SYS_readlinkat 305
#define SYS_fchmodat 306
#define SYS_faccessat 307
#endif /* SYS_openat */

#else /* __i386__ */

#ifndef SYS_openat
#error Add system call numbers for this platform
#endif

#endif /* __i386__ */
#endif /* __linux__ */

#endif /* AT_FDCWD */

#endif /* _ATFILE_SOURCE */

#endif /* __OPENAT_H */
