#!/usr/bin/env bash

# Create demo table in MySQL
mysql < /create-demo-table.sql && echo "[ $(date) ] MySQL demo table 'users' created."

# Setup DB for Hive access
# mysql -u root <<EOD
#     CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY 'Admin123.';
#     GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1';
#     FLUSH PRIVILEGES;
# EOD

mkdir -p /root/.mc/certs/CAs/
mkdir -p /home/mapr/.aws/

# Copy secure files from server
while [[ ! -f /home/mapr/tenant_user21_ticket.txt || ! -f /home/mapr/.aws/credentials ]]; do
    echo "Get ssl_truststore"
    sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/opt/mapr/conf/ssl_truststore /opt/mapr/conf/
    echo "Get ssl-client.xml"
    sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/opt/mapr/conf/ssl-client.xml /opt/mapr/conf/
    echo "Get chain-ca.pem"
    sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/opt/mapr/conf/ca/chain-ca.pem /root/.mc/certs/CAs/
    echo "Get credentials"
    sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/home/mapr/.aws/credentials /home/mapr/.aws/
    echo "Get user11 ticket"
    sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/home/mapr/tenant_user11_ticket.txt /home/mapr/
    echo "Get user21 ticket"
    sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/home/mapr/tenant_user21_ticket.txt /home/mapr/
    sleep 2
done

# Configure secure client
/opt/mapr/server/configure.sh -N maprdemo.mapr.io -c -secure -C mapr:7222 -OT mapr

# Obtain ticket for mapr user
cat /root/mapr_password | maprlogin password -user mapr
echo `getent hosts mapr` maprdemo.mapr.io >> /etc/hosts

# Setup S3 access
mkdir -p /root/.mc/certs/CAs; mkdir -p /home/mapr/.aws
chown -R mapr:mapr /home/mapr/.aws/

# Mount /mapr
mount -t nfs -o nolock mapr:/mapr /mapr
# mount -t nfs -o vers=4,proto=tcp,nolock,sec=sys mapr:/mapr /mapr

# S3 alias for mc
access_key=$(grep accessKey /home/mapr/.aws/credentials | awk '{ print $3 }')
secret_key=$(grep secretKey /home/mapr/.aws/credentials | awk '{ print $3 }')
curl -Lso /opt/mapr/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x /opt/mapr/bin/mc
/opt/mapr/bin/mc alias set df https://maprdemo.mapr.io:9000 $access_key $secret_key

# Add certificate to store
cp /root/.mc/certs/CAs/chain-ca.pem /usr/local/share/ca-certificates/chain-ca.crt
update-ca-certificates

# Enable sudo for root
echo "root ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/root
# Create users and groups for multi-tenant demo
getent group tenant1 || groupadd -g 10000 tenant1
getent group tenant2 || groupadd -g 20000 tenant2
id user11 || useradd -m -d /home/user11 -g 10000 -s /bin/bash -u 10001 user11
id user12 || useradd -m -d /home/user12 -g 10000 -s /bin/bash -u 10002 user12
id user21 || useradd -m -d /home/user21 -g 20000 -s /bin/bash -u 20002 user21
echo user11:mapr | chpasswd
echo user12:mapr | chpasswd
echo user21:mapr | chpasswd
mkdir /t1
mkdir /t2
# save the original fuse.conf
cp /opt/mapr/conf/fuse.conf /opt/mapr/conf/fuse.orig

echo "[ $(date) ] CREDENTIALS:"
echo "Cluster Admin: mapr/mapr"
echo "S3 Access Key: ${access_key}"
echo "S3 Secret Key: ${secret_key}"

# Run app
/app/.venv/bin/streamlit run main.py

# in case app crashes, keep container running for debug
sleep infinity
