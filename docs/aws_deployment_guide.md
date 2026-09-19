# Running the Airline Intelligent Data Platform on AWS

## Part 1: AWS Services You'll Use

### Core Services

| Service | What It Does | Why You Need It |
|---------|-------------|-----------------|
| **Amazon S3** | Object storage (like a cloud hard drive) | Stores your raw CSV/JSONL files, Delta tables, checkpoints, and all pipeline data |
| **AWS Databricks** | Managed Spark + Delta Lake platform | Runs all your notebooks, jobs, and SQL queries — same Databricks UI you'd use anywhere |
| **AWS IAM** | Identity & Access Management | Controls who/what can access your S3 buckets and Databricks workspace |
| **AWS VPC** | Virtual Private Cloud (networking) | Databricks creates clusters inside a VPC — isolates your compute from the public internet |
| **Amazon EC2** | Virtual machines | Databricks cluster nodes run on EC2 instances behind the scenes — you don't manage them directly |

### Optional / Advanced Services

| Service | What It Does | When You Need It |
|---------|-------------|-----------------|
| **Amazon MSK / Kinesis** | Managed Kafka / streaming ingestion | If you want to run the streaming pipeline with real event sources instead of simulators |
| **AWS Glue Data Catalog** | Hive-compatible metastore | Alternative to Databricks Unity Catalog for table metadata (free trial uses hive_metastore) |
| **Amazon QuickSight** | BI dashboarding (like Power BI) | Connect to Gold tables for visual reports — alternative to Power BI |
| **AWS Secrets Manager** | Store API keys and passwords | If you enable GenAI features and need to store LLM endpoint credentials |
| **Amazon CloudWatch** | Monitoring and alerting | Pipeline failure alerts, cluster health monitoring, cost tracking |
| **AWS Lambda** | Serverless functions | Trigger pipeline runs on file arrival in S3 (event-driven ingestion) |

### Cost Awareness (Free Tier)

| Service | Free Tier Allowance |
|---------|-------------------|
| S3 | 5 GB storage, 20K GET, 2K PUT requests/month (12 months) |
| EC2 | 750 hours/month of t2.micro or t3.micro (12 months) |
| Databricks | **14-day free trial** — includes workspace + compute credits |
| CloudWatch | 10 custom metrics, 10 alarms, 1M API requests/month |
| Glue Catalog | 1M objects stored, 1M requests/month |

> **Your synthetic dataset is small (~1 MB total).** The free trial gives you more than enough to run the full pipeline multiple times.

---

## Part 2: Use Cases for Each Service

### S3 — Your Data Lake Storage

```
s3://airline-ops-lakehouse/
├── raw/                    ← Upload your CSV/JSONL source files here
│   ├── flight_operations/
│   ├── airports/
│   ├── aircraft_reference/
│   └── weather_metar/
├── bronze/                 ← Delta tables written by Bronze ingestion
├── silver/                 ← Delta tables written by Silver transformation
├── gold/                   ← Delta tables written by Gold aggregation
├── checkpoints/            ← Spark Structured Streaming checkpoints
└── audit/                  ← Pipeline audit logs
```

**Use cases:**
- Landing zone for raw data files (CSV, JSONL, Parquet)
- Persistent storage for Delta Lake tables across all Medallion layers
- Streaming checkpoint storage for exactly-once processing
- Backup and versioning via S3 versioning + Delta time travel

### Databricks on AWS — Your Compute Engine

**Use cases:**
- Run PySpark notebooks for batch Bronze → Silver → Gold pipeline
- Run Spark Structured Streaming jobs for real-time ingestion
- Execute SQL analytics on Gold tables via Databricks SQL
- Schedule multi-task Workflows (DAGs) that chain notebooks
- Unity Catalog for data governance (on paid tier)

### Kinesis / MSK — Real-Time Streaming (Optional)

**Use cases:**
- Replace the simulator with real flight event streams
- Feed weather data from METAR APIs into Kinesis Data Streams
- CDC events from operational databases via Kinesis or Debezium → MSK
- The `streaming/streaming_ingestion.py` reads from these sources

### CloudWatch — Monitoring

**Use cases:**
- Alert when a pipeline job fails (integrate with audit log)
- Track cluster costs and auto-terminate idle clusters
- Monitor S3 storage growth over time
- Set billing alarms to avoid surprise charges

---

## Part 3: Step-by-Step AWS Deployment

### Step 1: Create an AWS Account

1. Go to [aws.amazon.com](https://aws.amazon.com) → **Create an AWS Account**
2. Enter email, password, payment method (won't be charged on free tier)
3. Select **Basic Support (Free)**
4. Sign in to the **AWS Management Console**

### Step 2: Create an S3 Bucket

1. Go to **S3** in the AWS Console
2. Click **Create bucket**
3. Settings:
   - Bucket name: `airline-ops-lakehouse-<your-initials>` (must be globally unique)
   - Region: `us-east-1` (cheapest, most services available)
   - Leave defaults (Block all public access = ON)
4. Click **Create bucket**

### Step 3: Upload Raw Data to S3

Using AWS CLI (install from [aws.amazon.com/cli](https://aws.amazon.com/cli)):

```bash
# Configure CLI with your access keys (IAM → Users → Security Credentials)
aws configure

# Upload raw data
aws s3 sync ./data/raw s3://airline-ops-lakehouse-<your-initials>/raw/csv/
aws s3 sync ./data/raw_json s3://airline-ops-lakehouse-<your-initials>/raw/json/
```

Or use the **S3 Console** → your bucket → **Upload** → drag and drop the `data/` folders.

### Step 4: Set Up Databricks on AWS

1. Go to [databricks.com/try-databricks](https://www.databricks.com/try-databricks)
2. Select **AWS** as your cloud provider
3. Choose **Start Free Trial** (14 days, no credit card for Databricks itself)
4. This creates:
   - A Databricks workspace URL (e.g., `https://dbc-xxxxx.cloud.databricks.com`)
   - An IAM cross-account role (Databricks manages EC2 clusters in your AWS account)
5. Follow the setup wizard — it will ask for your AWS account ID and create the necessary IAM roles

> **Important:** The Databricks free trial on AWS still uses YOUR AWS account for compute (EC2). You'll see EC2 charges on your AWS bill. Use small clusters (`i3.xlarge` single node) and auto-terminate after 30 minutes of inactivity.

### Step 5: Create a Cluster

1. In Databricks → **Compute** → **Create Cluster**
2. Settings:
   - Name: `airline-pipeline-cluster`
   - Runtime: **Databricks Runtime 13.3 LTS** or later
   - Node type: `i3.xlarge` (single node is fine)
   - Autoscaling: OFF (fixed at 1 worker for free trial)
   - Auto-terminate: **30 minutes**
3. Click **Create Cluster**

### Step 6: Connect the Git Repo

1. **Workspace** → **Repos** → **Add Repo**
2. Paste: `https://github.com/nileshsrivastava27/Airline-Flight-Operations.git`
3. Branch: `main`
4. Click **Create Repo**

Your repo is now at:
```
/Workspace/Repos/<your-email>/Airline-Flight-Operations/
```

### Step 7: Configure S3 Access

In a new notebook, configure Spark to read from your S3 bucket:

```python
# Option A: Instance profile (recommended — set in cluster config)
# No code needed if the cluster's IAM role has S3 access

# Option B: Direct credentials (quick for testing — NOT for production)
spark.conf.set("fs.s3a.access.key", "<YOUR_ACCESS_KEY>")
spark.conf.set("fs.s3a.secret.key", "<YOUR_SECRET_KEY>")

# Verify access
dbutils.fs.ls("s3://airline-ops-lakehouse-<your-initials>/raw/csv/")
```

**Recommended: Use an instance profile instead of hardcoding keys.**
1. Go to AWS IAM → Roles → Create role
2. Trusted entity: EC2
3. Attach policy: `AmazonS3FullAccess` (or a scoped policy for your bucket)
4. In Databricks → Compute → your cluster → Advanced → Instance Profile → add the role ARN

### Step 8: Update Data Paths and Run

Open `run_full_pipeline.py` and change the `REPO_ROOT` and data paths:

```python
REPO_ROOT = "/Workspace/Repos/<your-email>/Airline-Flight-Operations"

# If reading from S3 directly instead of repo files:
S3_DATA_ROOT = "s3://airline-ops-lakehouse-<your-initials>/raw"
```

Then run the notebook cells in order — same as the execution guide.

### Step 9: Create a Databricks Workflow (Job)

1. **Workflows** → **Create Job**
2. Job name: `airline_full_pipeline`
3. Add tasks in order:

| Task | Type | Path | Depends On |
|------|------|------|-----------|
| `ddl_setup` | Notebook | `run_full_pipeline` (cells 1–6) | — |
| `bronze_ingestion` | Notebook | `pipeline/databricks_bronze_ingestion` | ddl_setup |
| `bronze_validation` | SQL | `validation/bronze_validation.sql` | bronze_ingestion |
| `silver_transform` | Notebook | `pipeline/databricks_silver_transformation` | bronze_validation |
| `silver_validation` | SQL | `validation/silver_validation.sql` | silver_transform |
| `gold_transform` | Notebook | `pipeline/databricks_gold_transformation` | silver_validation |
| `gold_validation` | SQL | `validation/gold_validation.sql` | gold_transform |
| `genai_check` | Notebook | `genai/genai_setup_notebook` | gold_transform |

4. Cluster: Use the cluster you created, or create a job cluster (cheaper)
5. Schedule: Manual trigger or cron (e.g., daily at 6 AM)
6. Click **Create**

### Step 10: Verify and Screenshot

After the workflow runs:

1. **Workflows** → your job → click the run → see task results
2. **SQL Editor** → run validation queries against each layer
3. **Data Explorer** → browse `airline_ops` schemas and tables
4. Screenshot everything for your portfolio

---

## Part 4: AWS vs Azure Databricks Comparison

### Platform Setup

| Aspect | AWS Databricks | Azure Databricks |
|--------|---------------|-----------------|
| **Storage** | Amazon S3 | Azure Data Lake Storage Gen2 (ADLS) |
| **Compute** | EC2 instances | Azure VMs |
| **Networking** | VPC | VNet |
| **Identity** | IAM roles + instance profiles | Azure Active Directory (Entra ID) |
| **Metastore** | Glue Data Catalog or Unity Catalog | Azure-managed Unity Catalog |
| **Marketplace** | AWS Marketplace listing | Azure Marketplace listing |
| **Signup** | Databricks account + AWS account (separate) | Can create Databricks from Azure Portal directly |

### Ease of Setup

| Aspect | AWS | Azure | Winner |
|--------|-----|-------|--------|
| **Account creation** | Need separate Databricks + AWS accounts | Create Databricks workspace from Azure Portal in one flow | Azure |
| **Storage access** | Instance profiles or access keys — manual IAM setup | Managed identity or service principal — Azure handles trust | Azure |
| **Cluster launch** | Works but IAM cross-account role setup can be confusing | Smoother — Azure Resource Manager handles everything | Azure |
| **Git integration** | Same (Repos) | Same (Repos) | Tie |
| **Unity Catalog setup** | Manual S3 bucket + IAM for metastore | Azure auto-provisions storage for metastore | Azure |

### Cost Comparison (for this project's scale)

| Resource | AWS | Azure |
|----------|-----|-------|
| **Storage (5 GB)** | S3: ~$0.12/month | ADLS Gen2: ~$0.10/month |
| **Compute (i3.xlarge / Standard_DS3_v2)** | ~$0.312/hr (EC2) + ~$0.40/hr (DBU) | ~$0.25/hr (VM) + ~$0.40/hr (DBU) |
| **Free trial** | 14 days Databricks + AWS free tier | 14 days Databricks + Azure $200 credit |
| **Total for 1 pipeline run (~30 min)** | ~$0.35 | ~$0.32 |
| **Monthly if running daily** | ~$10–15 | ~$9–13 |

> **DBU = Databricks Unit** — Databricks' billing unit on top of cloud compute costs. Rates are similar on both clouds.

### Feature Parity

| Feature | AWS | Azure | Notes |
|---------|-----|-------|-------|
| **Delta Lake** | ✅ | ✅ | Identical |
| **Unity Catalog** | ✅ | ✅ | Identical |
| **Structured Streaming** | ✅ | ✅ | Identical |
| **Databricks SQL** | ✅ | ✅ | Identical |
| **Workflows (Jobs)** | ✅ | ✅ | Identical |
| **Vector Search** | ✅ | ✅ | Identical (paid tier) |
| **Model Serving** | ✅ | ✅ | Identical (paid tier) |
| **Photon engine** | ✅ | ✅ | Identical |
| **Serverless SQL** | ✅ | ✅ | Identical |
| **Native BI tool** | QuickSight (separate) | Power BI (tight integration) | Azure |
| **Event streaming** | Kinesis / MSK | Event Hubs | Both work with Spark |
| **Terraform support** | ✅ | ✅ | Both have Databricks Terraform provider |

### When to Pick Which

| Choose AWS When | Choose Azure When |
|----------------|-------------------|
| Your company already uses AWS | Your company already uses Azure / Microsoft 365 |
| You need tight S3 + Lambda event-driven pipelines | You need Power BI integration out of the box |
| Your team knows IAM roles and EC2 | Your team uses Azure Active Directory / Entra ID |
| You want Kinesis for streaming | You want Event Hubs for streaming |
| You're targeting AWS certifications | You're targeting Azure certifications |
| Cost optimization via spot instances is critical | You have Azure credits from Visual Studio / enterprise agreement |

### For Your Resume

> Both platforms run **identical Databricks code**. Your `pipeline/`, `streaming/`, and `genai/` modules work on both without changes — only the storage paths differ (`s3://` vs `abfss://`). Mentioning both clouds on your resume shows you understand the platform-agnostic nature of Databricks.

### What Changes Between AWS and Azure in Your Code

| What | AWS | Azure |
|------|-----|-------|
| **Storage path** | `s3://airline-ops-lakehouse/raw/` | `abfss://raw@airlineopsadls.dfs.core.windows.net/` |
| **Auth config** | `fs.s3a.access.key` / instance profile | `fs.azure.account.key` / managed identity |
| **Everything else** | Same PySpark, same Delta, same SQL | Same PySpark, same Delta, same SQL |

That's it. One line of path config is the only code difference.
