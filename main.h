/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __MAIN_H
#define __MAIN_H

#include <stdio.h>
#include <stddef.h>
#include <inttypes.h>

#define PRINT_FAIL printf("\a\e[1m\e[31m    **** ERROR ****  (%s:%d)\e[0m\n", __FILE__, __LINE__)
#define EXPECT_NEVER(label, args...) do { printf(label "\n", ##args); PRINT_FAIL; } while(0)
#define EXPECT_FAIL(label, result) do { printf(label " = %d (expect failure)\n", result); if(result >= 0) PRINT_FAIL; } while(0)
#define EXPECT_NOFAIL(label, result) do { printf(label " = %d\n", result); if(result < 0) PRINT_FAIL; } while(0)
#define EXPECT_NOFAIL_FORMAT(label, result, args...) do { printf(label " = %d\n", ##args, result); if(result < 0) PRINT_FAIL; } while(0)
#define EXPECT_NOFAIL_COUNT(label, result, name, value) do { printf(label " = %d, %zu " name "\n", result, value); if(result < 0) PRINT_FAIL; } while(0)
#define EXPECT_NONULL(label, ptr) do { printf(label " = %p\n", ptr); if(!ptr) PRINT_FAIL; } while(0)
#define EXPECT_SIZET(label, expect, test) do { size_t __value = test; printf(label " = %zu (expect %zu)\n", __value, (size_t) expect); if(__value != (expect)) PRINT_FAIL; } while(0)
#define EXPECT_NOTU32(label, expect, test) do { uint32_t __value = test; printf(label " = %u\n", __value); if(__value == (expect)) PRINT_FAIL; } while(0)

#define EXPECT_NOFAIL_SILENT_BREAK(label, result) if(result < 0) { EXPECT_NEVER(label " failure"); break; }
#define EXPECT_NOFAIL_SILENT_RETURN(label, result, cleanup...) if(result < 0) { EXPECT_NEVER(label " failure"); cleanup; return result; }

#ifdef __cplusplus
extern "C" {
#endif

/* in main_perf.cpp */
int command_odtable(int argc, const char * argv[]);
int command_ldtable(int argc, const char * argv[]);
int command_bfdtable(int argc, const char * argv[]);
int command_oracle(int argc, const char * argv[]);
int command_blob_cmp(int argc, const char * argv[]);
int command_performance(int argc, const char * argv[]);
int command_bdbtest(int argc, const char * argv[]);

/* in main_test.cpp */
int command_info(int argc, const char * argv[]);
int command_dtable(int argc, const char * argv[]);
int command_edtable(int argc, const char * argv[]);
int command_exdtable(int argc, const char * argv[]);
int command_ussdtable(int argc, const char * argv[]);
int command_sidtable(int argc, const char * argv[]);
int command_didtable(int argc, const char * argv[]);
int command_kddtable(int argc, const char * argv[]);
int command_udtable(int argc, const char * argv[]);
int command_ctable(int argc, const char * argv[]);
int command_cctable(int argc, const char * argv[]);
int command_consistency(int argc, const char * argv[]);
int command_durability(int argc, const char * argv[]);
int command_rollover(int argc, const char * argv[]);
int command_abort(int argc, const char * argv[]);
int command_stable(int argc, const char * argv[]);
int command_iterator(int argc, const char * argv[]);

#ifdef __cplusplus
}

#include <time.h>
#include <sys/time.h>

#include "dtable.h"
#include "ctable.h"
#include "stable.h"

/* in main_perf.cpp */
void abort_perf(bool use_temp);
void abort_effect(void);
void exdtable_perf(void);
int kddtable_perf(void);
int udtable_perf(void);
int blob_cmp_perf(blob_comparator * blob_cmp);

/* in main_util.cpp */
void print(const dtype & x);
void print(const blob & x, const char * prefix = NULL, ...);

void run_iterator(dtable::iter * iter);
void run_iterator(const dtable * table, ATX_OPT);
void run_iterator(const ctable * table);
void run_iterator(const stable * table);
void time_iterator(const dtable * table, size_t count = 1, ATX_OPT);

void timeval_subtract(struct timeval * end, const struct timeval * start);
void timeval_add(struct timeval * accumulator, const struct timeval * delta);
void timeval_divide(struct timeval * time, int denominator, bool round = false);

void print_timeval(const struct timeval * time, bool seconds = false, bool newline = false);
void print_elapsed(const struct timeval * start, struct timeval * end, bool elapsed = false);
void print_elapsed(const struct timeval * start, bool elapsed = false);
void print_progress(const struct timeval * start, struct timeval * now, int percent);
void print_progress(const struct timeval * start, int percent);

void wait_digest(int seconds);
blob random_blob(size_t size);

#endif /* __cplusplus */

#endif /* __MAIN_H */
