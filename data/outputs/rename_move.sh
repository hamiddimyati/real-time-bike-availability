#!/bin/bash

YEAR="$1"
SUFFIXE="$2"

for (( i=1; i<13; i++ ))
	do
		MONTH=$i
		if (( ${#i} == 1 ))
			then
				MONTH="0$i"
			fi
		FOLDER=$YEAR$MONTH$SUFFIXE
		mv $FOLDER/*.csv ./dataset/"$YEAR-$MONTH.csv"
	done
