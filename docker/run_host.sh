#!/bin/sh

USER="vycodi-host"
SUDO="sudo -u $USER -E PYTHONPATH=."

$SUDO mkdir host_run \
	&& $SUDO touch host_run/daemon.log \
	&& $SUDO python3 docker/run-with-redis.py \
	&& $SUDO python3 -m vycodi.cli host start --foreground docker/host_config.json \
	| tail -f host_run/daemon.log
