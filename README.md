
Iceberg Data Sharing Between AWS Accounts
Overview
This document outlines two distinct architectures for sharing Iceberg tables across AWS accounts using:

Federated Data Lake (FDL)

Glue Resource Policies with Cross-Account Access

Both approaches are designed to support data consumption from foundational services like Trino, DBT, and Kestra, and are optimized for analytical workloads using Athena, EMR, or AWS Glue.

🔁 Approach 1: Iceberg Data Sharing using Federated Data Lake (FDL)
🔷 Architecture Diagram

🔹 Description
This method leverages AWS Lake Formation and FDL to establish a controlled, centralized model of data sharing between AWS accounts.

🔸 Key Components
Producer Account (nonhcd)
Hosts source Iceberg data in S3 and local Glue catalog.
Tools like Trino, DBT, and Kestra interact with this stack.

Central Account (nonhcd)
Maintains the central Glue catalog for federated sharing.

Consumer Account (nonhcd)
Subscribes to the catalog and consumes data using Athena, EMR, or other engines.

🔹 Workflow Steps
Create LF DB in the Central Account for the shared dataset.

Create LF DB in the Producer Account to act as the local metadata store.

Attach IAM Role from Central to access the producer's S3 buckets.

AWS Glue Job in the Producer to sync metadata to local Glue DB.

Grant Permissions from Central to Consumer for metadata access.

Link DBs via FDL and enable consumers to query data.

✅ Pros
Centralized governance and entitlement control.

Scales well for multi-account data lakes.

Fine-grained access management via LF.

⚠️ Considerations
Slightly more complex to set up and automate.

Requires coordination between Producer, Central, and Consumer.

🔁 Approach 2: Iceberg Data Sharing using Glue Resource Policy
🔷 Architecture Diagram

🔹 Description
This approach directly shares data using Glue Resource Policies and cross-account IAM roles, without needing a central account.

🔸 Key Components
Producer Account (nonhcd)
Hosts Iceberg data and defines Glue Resource Policies to share catalog.

Consumer Account (nonhcd)
Sets up IAM role with permissions to access producer's Glue catalog and S3.

🔹 Workflow Steps
Enable Glue Resource Policy on the Producer to allow consumer access.

In Consumer account, update/create IAM Role with permissions:

Cross-account S3 bucket access

Glue key access

Glue catalog read permissions

Update S3 bucket policy in Producer to allow access to Consumer IAM role.

Once configured, Consumer can run Glue/Athena jobs directly on shared Iceberg tables.

✅ Pros
Simpler setup with fewer AWS accounts involved.

No dependency on a central catalog.

⚠️ Considerations
Less centralized governance.

Manual permission management is required for each consumer.

Best for controlled or limited sharing scenarios.

📊 Comparison Summary
Feature	FDL Approach	Glue Resource Policy Approach
Central Governance	✅ Yes (via central account)	❌ No
Ease of Setup	❌ Medium-High Complexity	✅ Simpler
Scalability	✅ Suitable for many consumers	⚠️ Limited
Fine-grained Access Control	✅ Lake Formation supported	⚠️ Requires IAM and bucket policies
Automation Support	✅ Glue Jobs, FDL Integration	✅ Glue Jobs supported
🔚 Conclusion
Choose the FDL approach when:

You require centralized access governance

There are multiple consumers

You are investing in Lake Formation for security and access control

Choose the Glue Resource Policy approach when:

You need a quick and lightweight integration

You’re working with a few known consumers

Centralized governance is not mandatory

