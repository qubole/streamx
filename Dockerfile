FROM confluentinc/cp-kafka:3.2.1

MAINTAINER pseluka@qubole.com

RUN apt-get update && apt-get install -y vim && apt-get -y install telnet

ENV STREAMX_DIR /usr/local/streamx

ADD target/streamx-0.1.0-SNAPSHOT-development/share/java/streamx $STREAMX_DIR
ADD config $STREAMX_DIR/config
ADD docker/entry $STREAMX_DIR/entry
ADD docker/utils.py $STREAMX_DIR/utils.py

EXPOSE 8083

ENV CLASSPATH=$CLASSPATH:$STREAMX_DIR/*
RUN cp /usr/local/streamx/*.jar /usr/bin/../share/java/kafka/

RUN chmod 777 $STREAMX_DIR/entry && mkdir /tmp/streamx-logs
CMD ["bash","-c","$STREAMX_DIR/entry"]
