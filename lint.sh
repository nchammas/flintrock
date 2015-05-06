#!/usr/bin/env bash
# echo "Errors"
python3 -m compileall ./flintrock
python3 -m pep8 ./flintrock

# Warnings. Don't fail the linter.
# echo "Warnings"
# python3 -m pep8 --select "E501" ./flintrock || true
