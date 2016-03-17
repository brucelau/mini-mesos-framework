#
# - This is the Dockerfile for the allocator's executor
#

FROM ubuntu:14.04

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
RUN echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list
#RUN apt-get update && apt-get install -y mesos  software-properties-common
#RUN add-apt-repository ppa:openjdk-r/ppa && apt-get install -y openjdk-8-jdk
RUN apt-get update && apt-get install -y mesos openjdk-7-jdk


COPY ./target/MiniFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar /opt/
COPY ./src/main/scripts/docker_executor /opt/
RUN shasum /opt/MiniFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar > /opt/MiniFramework.sha1

