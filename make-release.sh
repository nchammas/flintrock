# Update:
#   - Default Spark version: https://spark.apache.org/downloads.html
#   - Default Hadoop version: https://hadoop.apache.org/releases.html
#   - Default Amazon Linux 2 EBS AMI: https://aws.amazon.com/amazon-linux-2/release-notes/
#   - Dependencies: https://requires.io/github/nchammas/flintrock/requirements/?branch=master
# Run full acceptance tests
# Update Flintrock version
#   - flintrock/__init__.py
# Update CHANGES
#   - Check: https://github.com/nchammas/flintrock/pulls?q=is%3Apr+is%3Aclosed+label%3A%22needs+changelog%22
#   - Update "Unreleased" section. "Nothing notable yet."
# Tag release on GitHub

trash dist/ build/ Flintrock.egg-info/

python setup.py sdist bdist_wheel

# python setup.py register -r https://testpypi.python.org/pypi

# Test PyPI upload
twine upload dist/* --repository pypitest
open https://test.pypi.org/project/Flintrock/

# Production PyPI upload
twine upload dist/* --repository pypi
open https://pypi.org/project/Flintrock/

python generate-standalone-package.py

# open dist/
# Upload release builds to GitHub
#   - Wheel
#   - OS X standalone package
#   - Linux standalone package
# Update version to next.dev0

# ---

# Test release via pip
deactivate
trash venv
python3 -m venv venv
source venv/bin/activate

python3 -m pip install --extra-index-url https://testpypi.python.org/simple flintrock
