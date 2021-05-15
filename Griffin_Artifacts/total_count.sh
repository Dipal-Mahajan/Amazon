#!/bin/bash
#script to check the Total Count
set -e
set -x
path=/mnt/griffin
sudo su -l root -c "chmod -R 757 $path"

spark-submit --class org.apache.griffin.measure.Application --master yarn --deploy-mode client --queue default \
--driver-memory 5g --executor-memory 20g --num-executors 2 \
$path/griffin-measure.jar \
$path/env.json $path/dq.json

env_val=$(aws ssm get-parameter --name "/AdminParams/Team/Environment" | jq --raw-output '.Parameter["Value"]')
env_code="s3://gd-ckpetlbatch-"$env_val"-hadoop-migrated"

sudo su -l root -c "aws s3 cp --recursive $path/persist/total_count/ $env_code/griffin/output/total_count/"

done
