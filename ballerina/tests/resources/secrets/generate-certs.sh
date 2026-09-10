#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Generating SSL certificates for ActiveMQ testing..."

rm -f -- ./*.jks ./*.p12 ./*.pem ./*.key

echo "1. Creating server keystore..."
keytool -genkeypair -alias server -keyalg RSA -keysize 2048 \
    -dname "CN=localhost,OU=Test,O=Ballerina,L=Colombo,ST=Western,C=LK" \
    -validity 365 -keystore server-keystore.jks -storepass password -keypass password

echo "2. Exporting server certificate..."
keytool -exportcert -alias server -keystore server-keystore.jks \
    -storepass password -file server.crt.tmp

echo "3. Creating client keystore..."
keytool -genkeypair -alias client -keyalg RSA -keysize 2048 \
    -dname "CN=client,OU=Test,O=Ballerina,L=Colombo,ST=Western,C=LK" \
    -validity 365 -keystore client-keystore.jks.tmp -storetype JKS -storepass password -keypass password

echo "4. Exporting client certificate..."
keytool -exportcert -alias client -keystore client-keystore.jks.tmp \
    -storepass password -file client.crt.tmp

echo "5. Creating server truststore..."
keytool -importcert -alias client -file client.crt.tmp \
    -keystore server-truststore.jks -storepass password -noprompt

echo "6. Creating client truststore..."
keytool -importcert -alias server -file server.crt.tmp \
    -keystore client-truststore.jks.tmp -storetype JKS -storepass password -noprompt

echo "7. Converting client keystore to PKCS12..."
keytool -importkeystore -srckeystore client-keystore.jks.tmp -srcstorepass password \
    -destkeystore client-keystore.p12 -deststoretype PKCS12 -deststorepass password

echo "8. Converting client truststore to PKCS12..."
keytool -importkeystore -srckeystore client-truststore.jks.tmp -srcstorepass password \
    -destkeystore client-truststore.p12 -deststoretype PKCS12 -deststorepass password

echo "9. Exporting server certificate to PEM..."
keytool -exportcert -alias server -keystore server-keystore.jks \
    -storepass password -rfc -file server.pem

echo "10. Exporting client certificate to PEM..."
keytool -exportcert -alias client -keystore client-keystore.p12 \
    -storepass password -rfc -file client-cert.pem

echo "11. Extracting client private key..."
openssl pkcs12 -in client-keystore.p12 -passin pass:password \
    -nodes -nocerts -out client.key

echo "12. Keeping JKS copies of the client keystore/truststore..."
mv client-keystore.jks.tmp client-keystore.jks
mv client-truststore.jks.tmp client-truststore.jks

rm -f -- ./*.tmp

echo ""
echo "Certificate generation complete!"
echo ""
echo "Generated files:"
echo "  Server (for ActiveMQ broker):"
echo "    - server-keystore.jks (JKS keystore with server cert+key)"
echo "    - server-truststore.jks (JKS truststore with client cert)"
echo "    - server.pem (PEM format certificate for CertKey tests)"
echo ""
echo "  Client (for Ballerina tests):"
echo "    - client-keystore.p12 (PKCS12 keystore with client cert+key)"
echo "    - client-truststore.p12 (PKCS12 truststore with server cert)"
echo "    - client-keystore.jks (JKS keystore with client cert+key)"
echo "    - client-truststore.jks (JKS truststore with server cert)"
echo "    - client-cert.pem (PEM format certificate for CertKey tests)"
echo "    - client.key (PEM format private key for CertKey tests)"
echo ""
echo "All keystores use password: 'password'"
