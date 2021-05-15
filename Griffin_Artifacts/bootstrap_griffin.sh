set +e
set -x
echo "Starting the script"
sudo systemctl enable amazon-ssm-agent
echo "Enabled the SSM agent"
sudo systemctl start amazon-ssm-agent
echo "Started the SSM agent"
sudo systemctl daemon-reload
env_val=$(aws ssm get-parameter --name "/AdminParams/Team/Environment" | jq --raw-output '.Parameter["Value"]')
env_code="s3://gd-ckpetlbatch-"$env_val"-code"
path=/mnt/griffin
sudo su -l root -c "mkdir $path"
sudo su -l root -c "aws s3 sync $env_code/uds-griffin-validation/ref_fraud_order/ $path"
sudo su -l root -c "chmod -R 757 $path"
