# External modules
import pytest

# Flintrock modules
from flintrock.exceptions import (
    UsageError
)
from flintrock.flintrock import (
    option_name_to_variable_name,
    variable_name_to_option_name,
    option_requires,
    mutually_exclusive,
    get_latest_commit
)


def test_option_name_to_variable_name_conversions():
    test_cases = [
        ('--ec2-user', 'ec2_user'),
        ('--provider', 'provider'),
        ('--spark-git-commit', 'spark_git_commit')
    ]

    for option_name, variable_name in test_cases:
        assert option_name_to_variable_name(option_name) == variable_name
        assert variable_name_to_option_name(variable_name) == option_name
        assert option_name == variable_name_to_option_name(
            option_name_to_variable_name(option_name))
        assert variable_name == option_name_to_variable_name(
            variable_name_to_option_name(variable_name))


def test_option_requires():
    some_option = 'something'
    unset_option = None
    set_option = '와 짠이다'

    option_requires(
        option='--some-option',
        requires_all=['--set_option'],
        scope=locals()
    )

    option_requires(
        option='--some-option',
        requires_any=[
            '--set_option',
            '--unset-option'],
        scope=locals()
    )

    with pytest.raises(UsageError):
        option_requires(
            option='--some-option',
            requires_all=[
                '--set-option',
                '--unset-option'],
            scope=locals()
        )

    with pytest.raises(UsageError):
        option_requires(
            option='--some-option',
            requires_any=[
                '--unset-option'],
            scope=locals()
        )


def test_option_requires_conditional_value():
    unset_option = None
    set_option = '대박'

    some_option = 'magic'
    option_requires(
        option='--some-option',
        conditional_value='magic',
        requires_any=[
            '--set-option',
            '--unset-option'],
        scope=locals()
    )

    some_option = 'not magic'
    option_requires(
        option='--some-option',
        conditional_value='magic',
        requires_any=[
            '--unset-option'],
        scope=locals()
    )

    some_option = ''
    option_requires(
        option='--some-option',
        conditional_value='',
        requires_any=[
            '--unset-option'],
        scope=locals()
    )

    with pytest.raises(UsageError):
        some_option = 'magic'
        option_requires(
            option='--some-option',
            conditional_value='magic',
            requires_any=[
                '--unset-option'],
            scope=locals()
        )


def test_mutually_exclusive():
    option1 = 'yes'
    option2 = None

    mutually_exclusive(
        options=[
            '--option1',
            '--option2'],
        scope=locals())

    option2 = 'no'
    with pytest.raises(UsageError):
        mutually_exclusive(
            options=[
                '--option1',
                '--option2'],
            scope=locals())


def test_get_latest_commit():
    sha = get_latest_commit("https://github.com/apache/spark")
    assert len(sha) == 40

    with pytest.raises(UsageError):
        get_latest_commit("https://google.com")

    with pytest.raises(Exception):
        get_latest_commit("https://github.com/apache/nonexistent-repo")
