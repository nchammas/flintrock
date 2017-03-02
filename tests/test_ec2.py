import pytest
import click
from flintrock.ec2 import validate_tags


def test_validate_tags():
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
        ec2_tags = validate_tags(test_case[0])
        assert(isinstance(ec2_tags, list))
        for i, ec2_tag in enumerate(ec2_tags):
            expected_dict = test_case[1][i]
            for k in expected_dict:
                assert k in ec2_tag
                assert ec2_tag[k] == expected_dict[k]

    negative_test_cases = [["k1"], ["k2,v2,"], ["k3,,v3"], [",v4"]]
    for test_case in negative_test_cases:
        with pytest.raises(click.BadParameter):
            validate_tags(test_case)
