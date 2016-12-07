FROM confluentinc/cp-kafka:3.0.0

MAINTAINER pseluka@qubole.com

RUN apt-get update && apt-get install -y vim

ENV STREAMX_DIR /usr/local/streamx
ADD ["target/streamx-0.1.0-SNAPSHOT-development/share/java/streamx","/usr/local/streamx"]
ADD ["config","/usr/local/streamx/config"]
ENV CLASSPATH=$CLASSPATH:/usr/local/streamx/*

VOLUME ["/var/lib/streamx/log"]

ADD ["docker/entry", "/usr/local/streamx/entry"]
ADD ["docker/utils.py", "/usr/local/streamx/utils.py"]
RUN chmod 777 /usr/local/streamx/entry

EXPOSE 8083
CMD ["bash","/usr/local/streamx/entry"]
