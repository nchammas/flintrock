# Flintrock Tests

Use the tests in this directory to help you catch bugs as you work on Flintrock.

The instructions here assume the following things:

1. You're working from Flintrock's root directory.
2. You're working in a Python [virtual environment](https://docs.python.org/3/library/venv.html). (We set this up under `venv/` when we [installed Flintrock](../README.md#development-version).)
3. You're running Python 3.5+.
4. You've already setup your Flintrock config file and can launch clusters.


## Setup

Flintrock's tests have their own dependencies which you can install as follows:

```sh
python3 -m pip install -r requirements/developer.pip
```

Among other things, this will make [pytest](http://pytest.readthedocs.org/en/latest/) available at the command line. We'll use it to run our tests.


## Run All Tests

**Read through the rest of this document before you do this, so you know what the tests do.**

To run all of Flintrock's tests, just run:

```sh
py.test
```

Keep in mind that the complete test run is quite long.


## Static Analysis

These tests will make sure your code compiles, check for style issues, and look for other potential problems that can be detected without running Flintrock "for real".

```sh
py.test tests/test_static.py
```


## Acceptance Tests

These tests launch and manipulate real clusters to test Flintrock's various commands and make sure installed services like Spark are working correctly.

```sh
py.test tests/test_acceptance.py
```

Acceptance tests are the most valuable type of test for an orchestration tool like Flintrock, but they also **cost money** (less than $1 for the full test run) and take many minutes to run. Use them judiciously.

Note that, depending on your changes, **a failed test run may leave behind running clusters**. You'll need to delete these manually.
