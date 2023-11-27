# Update:
#   - Default Spark version: https://spark.apache.org/downloads.html
#   - Default Hadoop version: https://hadoop.apache.org/releases.html
#   - Default Amazon Linux 2 EBS AMI: https://aws.amazon.com/amazon-linux-2/release-notes/
aws ec2 describe-images \
    --owners amazon \
    --filters \
        "Name=name,Values=amzn2-ami-hvm-*-gp2" \
        "Name=root-device-type,Values=ebs" \
        "Name=virtualization-type,Values=hvm" \
        "Name=architecture,Values=x86_64" \
    --query \
        'reverse(sort_by(Images, &CreationDate))[:100].{CreationDate:CreationDate,ImageId:ImageId,Name:Name,Description:Description}'
#   - Dependencies: pip list --outdated
# Run full acceptance tests
#   - Run private VPC tests too
# Update Flintrock version
#   - flintrock/__init__.py
#   - README blurb about standalone version
# Update CHANGES
#   - Check: https://github.com/nchammas/flintrock/pulls?q=is%3Apr+is%3Aclosed+label%3A%22needs+changelog%22
#   - Update "Unreleased" section. "Nothing notable yet."
# Tag release on GitHub
#   - https://github.com/nchammas/flintrock/releases
#   - vX.Y.Z
#   - "Here's what's new in X.Y.Z."

trash dist/ build/ Flintrock.egg-info/

python -m build

# python setup.py register -r https://testpypi.python.org/pypi

# Test PyPI upload
twine upload dist/* --repository pypitest
open https://test.pypi.org/project/Flintrock/

# Production PyPI upload
twine upload dist/* --repository pypi
open https://pypi.org/project/Flintrock/

python generate-standalone-package.py

# Upload release builds to GitHub
open dist/
#   - Wheel
#   - macOS standalone package (x86 _and_ arm64?)
#   - Linux standalone package (built by CI)
# Update version to next.dev0

# ---

# Test release via pip
deactivate
trash venv
python3 -m venv venv
source venv/bin/activate

python3 -m pip install --extra-index-url https://testpypi.python.org/simple flintrock
