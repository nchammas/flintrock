# Update Flintrock version
# Update change log
# Tag release on GitHub

trash dist/ build/ Flintrock.egg-info/

python3 setup.py sdist bdist_wheel

# python3 setup.py register -r https://testpypi.python.org/pypi

twine upload dist/* --repository pypitest

python generate-standalone-package.py

# Upload standalone package to GitHub
# Update version to next.dev0
# Update CHANGES links

# ---

# Test release via pip

deactivate
trash venv

python3 -m venv venv
source venv/bin/activate

python3 -m pip install --extra-index-url https://testpypi.python.org/simple flintrock
