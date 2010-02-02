#!/bin/bash

[ "$N" ] || N=8

NL="`echo; echo N`"; NL="${NL%N}"

function set_up_test()
{
	echo
	echo "========== $1 ($2) =========="
	PATCH="bench/$1.patch"
	patch -p1 < $PATCH
	make -j2 || exit $?
}

function finish_test()
{
	PATCH="bench/$1.patch"
	patch -p1 -R < $PATCH
	echo "========== $1 ($2) =========="
	echo
}

function run_test_N()
{
	set_up_test "$1" "$2"
	local i=0
	while [ $i -lt $N ]
	do
		i=$((i + 1))
		echo "---------- $i/$N ----------"
		echo "$2" | ./anvil-local --bench
	done
	finish_test "$1" "$2"
}

function run_test_1()
{
	set_up_test "$1" "${2//$NL/; }"
	echo "$2" | ./anvil-local --bench
	finish_test "$1" "${2//$NL/; }"
}
