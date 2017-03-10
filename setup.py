import setuptools

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except ImportError:
    long_description = open('README.md').read()

from flintrock import __version__

setuptools.setup(
    name='Flintrock',
    version=__version__,
    description='A command-line tool for launching Apache Spark clusters.',
    long_description=long_description,
    url='https://github.com/nchammas/flintrock',
    author='Nicholas Chammas',
    author_email='nicholas.chammas@gmail.com',
    license='Apache License 2.0',

    # See: https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',

        'Topic :: Utilities',
        'Environment :: Console',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
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
        'boto3 == 1.4.4',
        'botocore == 1.5.10',
        'click == 6.7',
        'paramiko == 2.1.1',
        'PyYAML == 3.12',
        # This is to ensure that PyInstaller works. dateutil is an
        # indirect dependency of Flintrock, and PyInstaller chokes on
        # dateutil 2.5.0.
        # See: https://github.com/pyinstaller/pyinstaller/issues/1848
        'python-dateutil >= 2.5.3',
        # This is to address reports that PyInstaller-packaged versions
        # of Flintrock intermittently fail due to an out-of-date version
        # of Cryptography being used.
        # See: https://github.com/nchammas/flintrock/issues/169
        'cryptography >= 1.7.2',
    ],

    entry_points={
        'console_scripts': [
            'flintrock = flintrock.__main__:main',
        ],
    },
)
