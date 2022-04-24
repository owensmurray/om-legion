#!/bin/bash


for prog in node run stable scale-down; do
  (
    set -e -o pipefail
    mkdir -p docker-build.$$/bin
    cp test/Dockerfile-$prog docker-build.$$/Dockerfile
    cp -r "$(stack path --dist-dir)/build/om-legion-test-$prog/om-legion-test-$prog" docker-build.$$/bin

    cd docker-build.$$
    image=us.gcr.io/friendlee/om-legion-test-$prog:latest
    sudo docker build -t "$image" .
    sudo gcloud docker -- push "${image}"
    cd ../
    rm -rf docker-build.$$
  )
done
wait
