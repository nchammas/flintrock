#!/usr/bin/env bash
set -e

python3 -m compileall ./flintrock.py ./setup-ephemeral-storage.py
python3 -m pep8 ./flintrock.py ./setup-ephemeral-storage.py
