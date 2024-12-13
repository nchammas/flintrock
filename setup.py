import setuptools
# from flintrock import __version__


with open('README.md') as f:
    long_description = f.read()

setuptools.setup(
    name='Flintrock',
    # Moved to setup.cfg to avoid import of flintrock during installation of
    # flintrock. This used to work, but becomes a problem with isolated builds
    # and new pip behavior triggered by pyproject.toml.
    # version=__version__,
    description='A command-line tool for launching Apache Spark clusters.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nchammas/flintrock',
    author='Nicholas Chammas',
    author_email='nicholas.chammas@gmail.com',
    license='Apache License 2.0',
    python_requires='>= 3.9',

    # See: https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',

        'Topic :: Utilities',
        'Environment :: Console',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords=['Apache Spark'],

    packages=setuptools.find_packages(),
    include_package_data=True,

    # We pin dependencies because sometimes projects do not
    # strictly follow semantic versioning, so new "feature"
    # releases end up making backwards-incompatible changes.
    # Sometimes, new releases even introduce bugs which
    # totally break Flintrock.
    # For example: https://github.com/paramiko/paramiko/issues/615
    install_requires=[
        'boto3 == 1.29.4',
        'botocore == 1.32.4',
        'click == 8.1.7',
        'paramiko == 3.4.0',
        'PyYAML == 6.0.2',
    ],

    entry_points={
        'console_scripts': [
            'flintrock = flintrock.__main__:main',
        ],
    },
)
