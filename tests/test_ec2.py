import pytest
import click
from types import SimpleNamespace
import flintrock.ec2 as ec2


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
        ec2_tags = ec2.validate_tags(test_case[0])
        assert(isinstance(ec2_tags, list))
        for i, ec2_tag in enumerate(ec2_tags):
            expected_dict = test_case[1][i]
            for k in expected_dict:
                assert k in ec2_tag
                assert ec2_tag[k] == expected_dict[k]

    negative_test_cases = [["k1"], ["k2,v2,"], ["k3,,v3"], [",v4"]]
    for test_case in negative_test_cases:
        with pytest.raises(click.BadParameter):
            ec2.validate_tags(test_case)


def test_client_ip_address(monkeypatch):
    def mock_checkip(ret_ip: str):
        read_obj = SimpleNamespace()
        read_obj.decode = lambda _: ret_ip
        request_obj = SimpleNamespace()
        request_obj.read = lambda: read_obj
        monkeypatch.setattr(ec2.urllib.request, 'urlopen',
                            lambda url: request_obj)

    # Vide issue 271: https://github.com/nchammas/flintrock/issues/271
    mock_checkip('189.4.79.64, 107.167.109.191\n')
    assert ec2._get_client_ip_address() == '189.4.79.64'

    mock_checkip('189.4.79.64\n')
    assert ec2._get_client_ip_address() == '189.4.79.64'
