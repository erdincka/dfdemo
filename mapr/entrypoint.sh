#!/usr/bin/env bash

echo "[ $(date) ] Starting container configuration, watch logs and be patient, this will take a while!"

# Remove the while loop at the end so we can continue with the rest of the default init-script
sed -i '1,/This container IP/!d' /usr/bin/init-script
echo "[ $(date) ] Data Fabric configuring, this will take some time..."
/usr/bin/init-script 2>&1 > /root/configure-$(date +%Y%m%d_%H%M%S).log
echo "[ $(date) ] Data Fabric configuration is complete, preparing for demo..."

# Obtain ticket for mapr user
echo mapr | sudo -u mapr maprlogin password

# Set NiFi credentials
/opt/mapr/nifi/nifi-"${NIFI_VERSION}"/bin/nifi.sh set-single-user-credentials "${NIFI_USER}" "${NIFI_PASSWORD}"

if [ -n "${NIFI_WEB_PROXY_HOST}" ]; then
    sed -i "s|nifi.web.proxy.host=.*$|nifi.web.proxy.host=${NIFI_WEB_PROXY_HOST}|" /opt/mapr/nifi/nifi-${NIFI_VERSION}/conf/nifi.properties
    /opt/mapr/nifi/nifi-1.28.0/bin/nifi.sh restart 2>&1 >> /root/nifi-restart.log
    echo "[ $(date) ] NiFi set up to use proxy $NIFI_WEB_PROXY_HOST"
fi

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

echo "[ $(date) ] Mounting /mapr"
# /sbin/rpc.statd
# Mount /mapr
mount -t nfs -o nolock localhost:/mapr /mapr
# mount -t nfs -o vers=4,proto=tcp,nolock,sec=sys mapr:/mapr /mapr

echo "[ $(date) ] Setting up mc for S3"
# S3 alias for mc
/opt/mapr/bin/mc alias set df https://dfab.io:9000 $access_key $secret_key

echo "[ $(date) ] Creating demo volume, bucket, and stream"
maprcli volume create -name demovol -path /demovol -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false 
maprcli stream create -path /demovol/demostream -ttl 86400 -produceperm p -consumeperm p -topicperm p
/opt/mapr/bin/mc mb df/demobk

# Create users for multi-tenant demo
getent group tenant1 || groupadd -g 10000 tenant1
getent group tenant2 || groupadd -g 20000 tenant2
id user11 || useradd -m -d /home/user11 -g 10000 -s /bin/bash -u 10001 user11
id user12 || useradd -m -d /home/user12 -g 10000 -s /bin/bash -u 10002 user12
id user21 || useradd -m -d /home/user21 -g 20000 -s /bin/bash -u 20002 user21
echo user11:mapr | chpasswd
echo user12:mapr | chpasswd
echo user21:mapr | chpasswd
# Allow users access to system (login)
/opt/mapr/bin/maprcli acl set -type cluster -user root:fc mapr:fc user11:login user12:login user21:login
# /opt/mapr/bin/maprcli acl set -type volume -name tenant1Vol -user mapr:fc user11:fc user12:m


# Create volumes for multi-tenant demo
/opt/mapr/bin/maprcli volume create -name tenant1Vol -path /tenant1 -tenantuser user11 -readAce 'g:tenant1' -writeAce 'u:user11' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false 
/opt/mapr/bin/maprcli volume create -name tenant2Vol -path /tenant2 -tenantuser user21 -readAce 'g:tenant2' -writeAce 'u:user21' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false 
echo mapr | maprlogin generateticket -type tenant -user user11 -out /home/mapr/tenant_user11_ticket.txt
echo mapr | maprlogin generateticket -type tenant -user user21 -out /home/mapr/tenant_user21_ticket.txt
chown mapr:mapr /home/mapr/tenant_user11_ticket.txt /home/mapr/tenant_user21_ticket.txt
mkdir /mapr/dfab.io/tenant1/user11; chown user11:tenant1 /mapr/dfab.io/tenant1/user11
mkdir /mapr/dfab.io/tenant1/user12; chown user12:tenant1 /mapr/dfab.io/tenant1/user12
mkdir /mapr/dfab.io/tenant2/user21; chown user21:tenant2 /mapr/dfab.io/tenant2/user21

# Create Iceberg table on S3 bucket
# /opt/mapr/spark/spark-3.5.5/bin/pyspark \
#   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2 < /home/mapr/create_iceberg_table.py > /dev/null

echo "[ $(date) ] CREDENTIALS:"
# echo "Hive Credentials: hive/Admin123."
echo "NiFi: ${NIFI_USER}/${NIFI_PASSWORD}"
echo "Cluster Admin: mapr/mapr"
echo "S3 Access Key: ${access_key}"
echo "S3 Secret Key: ${secret_key}"

echo "[ $(date) ] Ready!"

sleep infinity # just in case, keep container running
