#!/bin/bash

if [[ "$1" == "" ]] ; then
    echo "First argument MUST be the docker image tag"
    exit 1
fi

docker run --name kadmin -e SPRING_PROFILES_ACTIVE=dockerlocal -p 8080:8080 -d bettercloudnonprod/kadmin-micro:$1

docker logs -f kadmin
