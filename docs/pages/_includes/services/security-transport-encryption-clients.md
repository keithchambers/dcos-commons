#### Transport Encryption Clients

With Transport Encryption enabled, service clients will need to be configured to use [the DC/OS CA bundle](https://docs.mesosphere.com/1.10/networking/tls-ssl/get-cert/) to verify the connections they make the service. Consult your client's documentation for trusting a CA and configure your client appropriately.