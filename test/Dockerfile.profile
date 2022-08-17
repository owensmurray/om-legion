FROM ubuntu:18.04
MAINTAINER rick@owensmurray.com

RUN apt-get update && apt-get install -y libgmp10 libleveldb1v5 psmisc netbase curl

ADD /bin /bin

CMD [ \
    "bash", \
    "-c", \
    "/bin/om-legion-test-node +RTS -L25 -hr -p -l -RTS; sleep 1000" \
  ]


