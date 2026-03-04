# GDPR Data Masking UDFs for Confluent Cloud

## Problem Description

A fintech company processing payment transactions on Confluent Cloud needs to comply with GDPR. They need custom Flink SQL functions to mask sensitive data before it lands in their analytics Kafka topics. The data engineering team needs:

1. **A scalar UDF in Java** called `MaskEmail` that takes an email string and returns a masked version (e.g., `john.doe@example.com` → `j***e@example.com`). It must handle null inputs gracefully.

2. **A table function (UDTF) in Java** called `ExtractPiiFields` that takes a JSON payload string and emits one row per detected PII field, with columns `field_name` (STRING) and `field_value` (STRING). It should detect fields like "email", "phone", "ssn" from the JSON keys.

3. **A Python scalar UDF** called `classify_risk` that takes a transaction amount (DOUBLE) and country code (STRING) and returns a risk level: "HIGH" if amount > 10000 or country is in a high-risk list, "MEDIUM" if amount > 1000, "LOW" otherwise.

4. **The Maven POM** needed to build the Java UDFs into a deployable JAR.

5. **The Confluent Cloud deployment commands** (CLI) to upload the JAR artifact and register both functions, plus example SQL queries using each function.

Write the Java source files, the Python UDF file, the Maven POM, and a deployment script with all CLI commands and SQL statements.

## Output Specification

- `MaskEmail.java` — Java scalar UDF implementation
- `ExtractPiiFields.java` — Java table function implementation
- `classify_risk.py` — Python scalar UDF
- `pom.xml` — Maven build file for the Java UDFs
- `deploy.sh` — Shell script with Confluent CLI commands for artifact upload, function registration, and example queries
