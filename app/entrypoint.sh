#!/usr/bin/env bash

# Create demo table in MySQL
mysql < /create-demo-table.sql && echo "[ $(date) ] MySQL demo table 'users' created."

# Setup DB for Hive access
# mysql -u root <<EOD
#     CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY 'Admin123.';
#     GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1';
#     FLUSH PRIVILEGES;
# EOD

# Copy secure files from server
sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/opt/mapr/conf/ssl_truststore /opt/mapr/conf/
sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/opt/mapr/conf/ssl-client.xml /opt/mapr/conf/
sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/opt/mapr/conf/ca/chain-ca.pem /root/.mc/certs/CAs/
sshpass -f /root/mapr_password scp -o StrictHostKeyChecking=no mapr@mapr:/home/mapr/.aws/credentials /home/mapr/.aws/

# Configure secure client
/opt/mapr/server/configure.sh -N dfab.io -c -secure -C mapr:7222

# Obtain ticket for mapr user
cat /root/mapr_password | maprlogin password -user mapr
echo `getent hosts mapr` dfab.io >> /etc/hosts

# Setup S3 access
mkdir -p /root/.mc/certs/CAs; mkdir -p /home/mapr/.aws
chown -R mapr:mapr /home/mapr/.aws/

# Mount /mapr
mount -t nfs -o nolock mapr:/mapr /mapr

# S3 alias for mc
access_key=$(grep accessKey /home/mapr/.aws/credentials | awk '{ print $3 }')
secret_key=$(grep secretKey /home/mapr/.aws/credentials | awk '{ print $3 }')
curl -Lso /opt/mapr/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x /opt/mapr/bin/mc
/opt/mapr/bin/mc alias set df https://dfab.io:9000 $access_key $secret_key

# Add certificate to store
cp /root/.mc/certs/CAs/chain-ca.pem /usr/local/share/ca-certificates/chain-ca.crt
update-ca-certificates

echo "[ $(date) ] CREDENTIALS:"
echo "NiFi: ${NIFI_USER}/${NIFI_PASSWORD}"
echo "Cluster Admin: mapr/mapr"
echo "S3 Access Key: ${access_key}"
echo "S3 Secret Key: ${secret_key}"

# Run app
./venv/bin/streamlit run main.py

# in case app crashes, keep container running for debug
sleep infinity
