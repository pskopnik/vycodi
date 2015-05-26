FROM ubuntu:14.04
MAINTAINER Paul Skopnik <paul@skopnik.me>
RUN groupadd -r vycodi-host && useradd -r -g vycodi-host vycodi-host
RUN apt-get update && apt-get -y install python3 python3-pip git
COPY requirements.txt docs/host_config.json run-with-redis.py /opt/vycodi/
RUN pip3 install -r /opt/vycodi/requirements.txt
COPY vycodi /opt/vycodi/vycodi
WORKDIR /opt/vycodi
RUN chown -R vycodi-host:vycodi-host .

CMD = ['/bin/bash', '-c', 'python3 run-with-redis.py && sudo -u vycodi-host python3 -m vycodi.cli host start --foreground host_config.json']
EXPOSE 9898
