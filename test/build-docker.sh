#!/bin/bash


(
  set -e -o pipefail
  mkdir -p docker-build.$$/bin
  cp test/Dockerfile$1 docker-build.$$/Dockerfile
  for prog in node run stable inc; do
    cp -r "$(stack path --dist-dir)/build/om-legion-test-$prog/om-legion-test-$prog" docker-build.$$/bin
  done
  wait

  cd docker-build.$$
  image=us.gcr.io/friendlee/om-legion-test:latest
  sudo docker build -t "$image" .
  sudo gcloud docker -- push "${image}"
  cd ../
  rm -rf docker-build.$$
)
