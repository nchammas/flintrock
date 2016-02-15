# Flintrock Tests

Use the tests in this directory to help you catch bugs as you work on Flintrock.

The instructions here assume the following things:

1. You've read through our [guide on contributing code](../CONTRIBUTING.md#contributing-code) and installed Flintrock's development dependencies.
2. You're working from Flintrock's root directory.
3. You're running Python 3.5+.
4. You've already setup your Flintrock config file and can launch clusters.


## Run All Tests

To run all of Flintrock's tests, just run:

```sh
py.test
```

Keep in mind that the complete test run is quite long, as **it involves launching real clusters which cost real money**.


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

Acceptance tests are the most valuable type of test for an orchestration tool like Flintrock, but they also **cost money** (less than $1 for the full test run) and take a while to run (~30-60 minutes). Use them judiciously.

Note that **a failed test run may leave behind running clusters**. You'll need to delete these manually.
