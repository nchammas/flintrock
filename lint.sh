#!/usr/bin/env bash
# echo "Errors"
python3 -m compileall ./flintrock.py
python3 -m pep8 ./flintrock.py

# Warnings. Don't fail the linter.
# echo "Warnings"
# python3 -m pep8 --select "E501" ./flintrock || true
