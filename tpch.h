/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TPCH_H
#define __TPCH_H

#ifdef __cplusplus
extern "C" {
#endif

int command_tpchtype(int argc, const char * argv[]);
int command_tpchgen(int argc, const char * argv[]);
int command_tpchopen(int argc, const char * argv[]);
int command_tpchtest(int argc, const char * argv[]);

#ifdef __cplusplus
}
#endif

#endif /* __TPCH_H */
