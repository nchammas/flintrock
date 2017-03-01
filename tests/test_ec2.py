import pytest
import click
from click.testing import CliRunner

from flintrock.ec2 import validate_tags


def test_validate_tags():
    @click.command()
    @click.option('--ec2-tag', 'ec2_tags',
                  callback=validate_tags,
                  multiple=True,
                  help="Additional tags (e.g. 'Key,Value') to assign to the instances. "
                       "You can specify this option multiple times.")
    def cli(ec2_tags):
        click.echo(ec2_tags)

    runner = CliRunner()

    # cases where validation and parsing should return formatted tags
    positive_test_cases = [("k1,v1", "'Key': 'k1'", "'Value': 'v1'"),
                           ("k2, v2 ", "'Key': 'k2'", "'Value': 'v2'"),
                           ("k3,", "'Key': 'k3'", "'Value': ''")]
    for test_case in positive_test_cases:
        result = runner.invoke(cli, ["--ec2-tag", test_case[0]])
        assert test_case[1] in result.output and test_case[2] in result.output
        assert result.exit_code == 0

    # one case where multiple tags are supplied
    result = runner.invoke(cli, ["--ec2-tag", 'k1,v1', "--ec2-tag", 'k2,v2'])
    assert "'Key': 'k1'" in result.output and "'Key': 'k2'" in result.output
    assert result.exit_code == 0

    # cases where validation should return error
    negative_test_cases = ["k1", "k2,v2,", "k3,,v3", ",v4"]
    for test_case in negative_test_cases:
        result = runner.invoke(cli, ["--ec2-tag", test_case])
        assert result.output.startswith("Usage:")
        assert result.exit_code == 2


def test_validate_args2():
    # List of test cases; each test case is a tuple, with first element
    # the input and the second element the expected output
    positive_test_cases = [
        # basic case
        (['k1,v1'], [{'Key': 'k1', 'Value': 'v1'}]),

        # strips whitespace?
        (['k2, v2 '], [{'Key': 'k2', 'Value': 'v2'}]),

        # empty Value
        (['k3,'], [{'Key': 'k3', 'Value': ''}]),

        # multiple tags
        (['k4,v4', 'k5,v5'],
         [{'Key': 'k4', 'Value': 'v4'}, {'Key': 'k5', 'Value': 'v5'}])]

    for test_case in positive_test_cases:
        ec2_tags = validate_tags(None, None, test_case[0])
        assert(isinstance(ec2_tags, list))
        for i, ec2_tag in enumerate(ec2_tags):
            expected_dict = test_case[1][i]
            for k in expected_dict:
                assert k in ec2_tag
                assert ec2_tag[k] == expected_dict[k]

    negative_test_cases = [["k1"], ["k2,v2,"], ["k3,,v3"], [",v4"]]
    for test_case in negative_test_cases:
        with pytest.raises(click.BadParameter):
            validate_tags(None, None, test_case)
