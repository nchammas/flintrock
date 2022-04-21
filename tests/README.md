# Flintrock Tests

Use the tests in this directory to help you catch bugs as you work on Flintrock.

The instructions here assume the following things:

1. You've read through our [guide on contributing code](../CONTRIBUTING.md#contributing-code) and installed Flintrock's development dependencies.
2. You're working from Flintrock's root directory.
3. You're running Python 3.7+.
4. You've already setup your Flintrock config file and can launch clusters.

To run all of Flintrock's tests that don't require AWS credentials, just run:

```sh
pytest
```

This is probably what you want to do most of the time.

To run all of Flintrock's tests, including the ones that require AWS credentials (like acceptance tests), run this:

```sh
USE_AWS_CREDENTIALS=true pytest  # will launch real clusters!
```

Acceptance tests launch and manipulate real clusters to test Flintrock's various commands and make sure installed services like Spark are working correctly.

Some things you should keep in mind when running the full test suite with your AWS credentials:

  * **Running the full test suite costs money** (less than $1 for the full test run) since it launches and manipulates real clusters.
  * **A failed test run may leave behind running clusters**. You'll need to destroy these manually.
  * The full test suite takes a while to run (~30-60 minutes).
  * Though the tests that use your AWS credentials are disabled by default, you can explicitly disable them by setting `USE_AWS_CREDENTIALS=""`. Setting that variable to `false` or to any non-empty string won't work.

Relatively speaking, acceptance tests are expensive, but they are the most valuable type of test for an orchestration tool like Flintrock. Use them judiciously.
