#!/bin/bash

NUMTHREAD=2
CHUNK="qc_chunk"
TABLE="CIQ_1_QUICKCOMP"

python quickcomp_main.py $CHUNK $NUMTHREAD

if [ $? -eq 0 ]
then
    	echo 'running each requestrs...'
	pids=()

	for (( i=1; i<=$NUMTHREAD; i++))
	do
		python quickcomp_sub.py $CHUNK $i  2>&1 & 
		pids+=($!)
		if [ $? -ne 0 ]
		then
			echo "***** Thread Script exited with error"
			break
		fi
	done
	wait

	for p in "${pids[@]}"
	do
		while kill -0 p >/dev/null 2>&1; do
			continue
		done
		echo "PROCESS $p TERMINATED"
	done	
	python quickcomp_union.py $NUMTHREAD $TABLE

else
	echo "***** Quickcomp Main Script exited with error." >&2
fi

