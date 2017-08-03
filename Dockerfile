FROM ubuntu:latest

MAINTAINER Bellhops, Inc. <data@getbellhops.com>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update && apt-get install -y wget nano locales curl unzip wget openssl libhdf5-dev libpq-dev python3-pip tzdata \
    && apt-get clean && dpkg-reconfigure locales && locale-gen en_US.UTF-8 \
    && echo "America/New_York" > /etc/timezone && dpkg-reconfigure --frontend noninteractive tzdata \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Get the pip packages and clean up
ADD requirements.txt /
RUN pip3 install -r /requirements.txt && rm -rf /root/.cache/pip/*

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8


WORKDIR /src
ADD src/* ./
