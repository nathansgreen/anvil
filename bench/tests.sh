#!/bin/bash

# excp_excp_array_x1k takes a really long time and is listed last so it can be run fewer times easily
TESTS_N=(excp_simple    excp_btree_simple excp_array      excp_excp_array excp_excp_fixed excp_excp_btree_fixed abort        uniq           partition       overlay bloom    oracle                    journal acid         excp_btree_simple_x1k excp_excp_array_x1k)
CMDS_N=("exdtable perf" "exdtable perf"   "exdtable perf" "exdtable perf" "exdtable perf" "exdtable perf"       "abort perf" "udtable perf" "kddtable perf" odtable bfdtable "oracle${NL}oracle bloom" jdtable "rwatx perf" "exdtable perf"       "exdtable perf")

TESTS_1=(tpch_run_row                  tpch_run_col                      tpch_create_fg_st          tpch_create_fg             tpch_create_bg2            tpch_create_bg)
CMDS_1=("tpchtype row${NL}tpchtest $N" "tpchtype column${NL}tpchtest $N" "tpchtype row${NL}tpchgen" "tpchtype row${NL}tpchgen" "tpchtype row${NL}tpchgen" "tpchtype row${NL}tpchgen")

TESTS_S=(tpch_create_row                        tpch_create_col)
CMDS_S=("tpchtype row${NL}tpchgen${NL}tpchopen" "tpchtype column${NL}tpchgen${NL}tpchopen")
