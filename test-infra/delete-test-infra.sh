#!/usr/bin/env bash
set -e
set -x

# Apparently you can't simply do `terraform state show aws_vpc.main.id`.
vpc_id="$(
    terraform show -json \
    | jq --raw-output '
        .values.root_module.resources[] 
        | select(.type == "aws_vpc" and .name == "main") 
        | .values.id
    '
)"

security_group_ids=($(
    aws ec2 describe-security-groups \
        --filters "Name=vpc-id,Values=$vpc_id" "Name=group-name,Values=flintrock" \
        --query "SecurityGroups[*].{ID:GroupId}" \
    | jq --raw-output '.[] | .ID'
))

for sg_id in "${security_group_ids[@]}"; do
    aws ec2 delete-security-group --group-id "$sg_id"
done

terraform destroy
