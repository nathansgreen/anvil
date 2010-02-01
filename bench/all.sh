#!/bin/bash

[ -d bench ] || exit 1

source bench/test.sh

# excp_excp_array_x1k takes a really long time and is listed last so it can be run fewer times easily
TESTS_N=(excp_simple    excp_btree_simple excp_array      excp_excp_array excp_excp_fixed excp_excp_btree_fixed abort        uniq           partition       overlay bloom    oracle                    journal acid         excp_btree_simple_x1k excp_excp_array_x1k)
CMDS_N=("exdtable perf" "exdtable perf"   "exdtable perf" "exdtable perf" "exdtable perf" "exdtable perf"       "abort perf" "udtable perf" "kddtable perf" odtable bfdtable "oracle${NL}oracle bloom" jdtable "rwatx perf" "exdtable perf"       "exdtable perf")

TESTS_1=(tpch_run_row                  tpch_run_col                      tpch_create_fg_st          tpch_create_fg             tpch_create_bg2            tpch_create_bg)
CMDS_1=("tpchtype row${NL}tpchtest $N" "tpchtype column${NL}tpchtest $N" "tpchtype row${NL}tpchgen" "tpchtype row${NL}tpchgen" "tpchtype row${NL}tpchgen" "tpchtype row${NL}tpchgen")

TESTS_S=(tpch_create_row                        tpch_create_col)
CMDS_S=("tpchtype row${NL}tpchgen${NL}tpchopen" "tpchtype column${NL}tpchgen${NL}tpchopen")

if [ "$1" = "--setup" ]
then
	i=0
	while [ $i -lt ${#TESTS_S[*]} ]
	do
		run_test_1 "${TESTS_S[$i]}" "${CMDS_S[$i]}"
		echo -n "Waiting to continue. Press enter. "; read
		i=$((i + 1))
	done
	exit 0
fi

HOSTNAME="`hostname`"

if [ "$1" != "--one" ]
then
	i=0
	while [ $i -lt ${#TESTS_N[*]} ]
	do
		echo "Starting test: ${TESTS_N[$i]}" | mail -s "$HOSTNAME: $0 status update" $USER
		# When we get to this test, do only 2 runs instead of the default
		[ "${TESTS_N[$i]}" = "excp_excp_array_x1k" ] && N=2
		run_test_N "${TESTS_N[$i]}" "${CMDS_N[$i]}"
		i=$((i + 1))
	done
fi

i=0
while [ $i -lt ${#TESTS_1[*]} ]
do
	echo "Starting test: ${TESTS_1[$i]}" | mail -s "$HOSTNAME: $0 status update" $USER
	run_test_1 "${TESTS_1[$i]}" "${CMDS_1[$i]}"
	i=$((i + 1))
done

echo "All tests finished" | mail -s "$HOSTNAME: $0 finished" $USER
