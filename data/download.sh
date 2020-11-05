#!/bin/bash
while IFS= read -r line
do
	wget "$line"
done < "$1"
