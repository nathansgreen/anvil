#!/bin/bash

DO_N=no

if [ "$1" = "-N" ]
then
	DO_N=yes
	shift
fi

[ -d bench ] || exit 1

source bench/test.sh

[ -f "bench/$1.patch" ] || exit 2
[ $# = 2 ] || exit 3

if [ "$DO_N" = "yes" ]
then
	run_test_N "$1" "$2"
else
	run_test_1 "$1" "$2"
fi
