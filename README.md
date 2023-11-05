# Streaming Data Warehouse

## Enigineering thesis | Silesian University of Technology

Streaming data warehouse for stock market data with Airflow, Kafka, Spark, Snowflake

Running:
`docker compose up -d --build`


How to access:

Apache Airflow
`localhost:8080`

Apache Kafka
`localhost:9021`

Apache Spark
`localhost:9090`

Apache Jobs
`localhost:4040`


Create `.env` file with these informations

```
SF_URL=https://<account_name>.snowflakecomputing.com
SF_ACCOUNT=<account_name>
SF_USER=<username>
SF_PASSWORD=<password>
SF_DATABASE=<database_name>
SF_SCHEMA=<schema_name>
SF_WAREHOUSE=<warehouse_name>
PKB=<pkb_key>
```


# Generating Public and Private Keys

## Step 1: Generate the Public and Private Key

You can generate either an encrypted version of the private key or an unencrypted version of the private key.

### Option A: Unencrypted Version

To generate an unencrypted version of the private key, use the following command:

```bash
openssl genrsa -out rsa_key.pem 2048
```

To generate an unencrypted version of the public key, use the following command:

```bash
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
```

### Option B: Encrypted Version

To generate an encrypted version of the private key, use the following command:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
```

To generate an encrypted version of the public key, use the following command:

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

## Step 2: Assign the Public Key to the Snowflake User

Use the `ACCOUNTADMIN` role to assign the public key to the Snowflake user using the ALTER USER command. For example:

```sql
ALTER USER <user> SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```

## Step 3: Generate a key and assign it in .env

Generate a PKB key via `utils/generate_key.py`

Set as a `PKB` variable in `.env` file.


# Images

![Power BI](images/power_bi.png)