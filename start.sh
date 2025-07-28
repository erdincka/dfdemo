#!/usr/bin/env bash

sed -i '1,/This container IP/!d' /usr/bin/init-script # remove the while loop at the end
echo "[ $(date) ] Data Fabric configuring, this will take some time..."
/usr/bin/init-script 2>&1 > /root/configure-$(date +%Y%m%d_%H%M%S).log
echo "[ $(date) ] Data Fabric configuration is complete, preparing for demo..."

# Obtain ticket for mapr user
echo mapr | sudo -u mapr maprlogin password

# Setup Object Store
mkdir -p /root/.mc/certs/CAs/; cp /opt/mapr/conf/ca/chain-ca.pem /root/.mc/certs/CAs/
AWS_CREDS=$(maprcli s3keys generate -domainname primary -accountname default -username mapr)
read -r access_key secret_key <<<$(echo "$AWS_CREDS" | grep -v accesskey)
mkdir -p /home/mapr/.aws
echo """
[default]
aws_access_key_id = ${access_key}
aws_secret_access_key = ${secret_key}
accessKey = ${access_key}
secretKey = ${secret_key}
""" > /home/mapr/.aws/credentials
chown -R mapr:mapr /home/mapr/.aws/

# Mount locally
mount -t nfs -o nolock mapr:/mapr /mapr
# S3 alias for mc
/opt/mapr/bin/mc alias set df https://dfab.io:9000 $access_key $secret_key
/opt/mapr/bin/mc mb df/demobk

LD_LIBRARY_PATH=/opt/mapr/lib nohup /app/.venv/bin/streamlit run /app/main.py &

echo "[ $(date) ] CREDENTIALS:"
# echo "Hive Credentials: hive/Admin123."
echo "Cluster Admin: mapr/mapr"
echo "S3 Access Key: ${access_key}"
echo "S3 Secret Key: ${secret_key}"

echo "[ $(date) ] Ready!"
sleep infinity # just in case, keep container running
