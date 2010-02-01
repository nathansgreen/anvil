#!/bin/bash

DO_N=no

[ -d bench ] || exit 1

source bench/test.sh
source bench/tests.sh

[ -f "bench/$1.patch" ] || exit 2

CMD=
function find_command()
{
	local i=0
	eval "local max=\${#TESTS_$1[*]}"
	while [ $i -lt $max ]
	do
		local name="TESTS_$1[$i]"
		if [ "${!name}" = "$2" ]
		then
			name="CMDS_$1[$i]"
			CMD="${!name}"
			return 0
		fi
		i=$((i + 1))
	done
	return 1
}
find_command N "$1" && DO_N=yes || find_command 1 "$1" || find_command S "$1" || exit 3

if [ "$DO_N" = "yes" ]
then
	run_test_N "$1" "$CMD"
else
	run_test_1 "$1" "$CMD"
fi
