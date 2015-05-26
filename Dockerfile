FROM ubuntu:14.04
MAINTAINER Paul Skopnik <paul@skopnik.me>
RUN groupadd -r vycodi-host && useradd -r -g vycodi-host vycodi-host
RUN apt-get update && apt-get -y install python3 python3-pip git
COPY requirements.txt /opt/vycodi/
RUN pip3 install -r /opt/vycodi/requirements.txt
COPY vycodi /opt/vycodi/vycodi
COPY docker /opt/vycodi/docker
WORKDIR /opt/vycodi
RUN chown -R vycodi-host:vycodi-host .
RUN chmod a+x docker/*.sh

CMD docker/run_host.sh
EXPOSE 9898
