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
        'Development Status :: 3 - Alpha',

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
    install_requires=[
        'boto == 2.38.0',
        'click == 5.1',
        'paramiko == 1.15.4',
        'PyYAML == 3.11'
    ],

    entry_points={
        'console_scripts': [
            'flintrock = flintrock.__main__:main',
        ],
    },
)
