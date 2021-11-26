#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

export ZKSERVER=lab2-25.cs.mcgill.ca:21823,lab2-26.cs.mcgill.ca:21823,lab2-28.cs.mcgill.ca:21823

java -cp $CLASSPATH:../task:.: DistClient "$@"
