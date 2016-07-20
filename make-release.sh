# Run full acceptance tests
# Update Flintrock version
# Update README Spark and Flintrock version defaults
# Update CHANGES
# Tag release on GitHub

trash dist/ build/ Flintrock.egg-info/

python setup.py sdist bdist_wheel

# python setup.py register -r https://testpypi.python.org/pypi

twine upload dist/* --repository pypitest

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
