#!/bin/bash

[ -d bench ] || exit 1

source bench/test.sh
source bench/tests.sh

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
