# PHAROSKART — Data Architecture Optimization Strategy

> A Big Data case study implementing scalable analytics, unified customer insights,
> and real-time data streaming for a fast-growing international e-commerce platform.

---

## Team Members

| Name | Register Number | Role |
|---|---|---|
| Shri Harsan M | RA2512052010014 | Data Engineering & Pipeline Architecture |
| Madhusudhanan G | RA2512052010004 | Spark Processing & Warehouse Design |
| Pharos Sophy Samuel TJ | RA25120520018 | Kafka Streaming & Security Layer |
| Donald | RA2512052010059 | Data Lake & HDFS Management |
| ArunDas | RA2512052010011 | Data Generation & Analytics |

---

## Problem Statement

PHAROSKART generates massive volumes of data daily across 4 sources:

| Source | Volume | Type |
|---|---|---|
| Web logs | 2TB/day | Unstructured |
| Transactions | 500GB/day | Structured |
| Reviews | 200GB/day | Semi-structured |
| Social media | 100GB/day | Unstructured |

### Critical Challenges

- **Slow Reporting** — OLTP systems overloaded with analytical queries, reports taking minutes or hours
- **No Single Customer View** — customer data fragmented across multiple systems with no unified identification
- **Data Engineering Complexity** — no standardized pipelines, terabyte-scale unstructured data handling
- **Security & Privacy Risks** — weak access control, lack of centralized governance

---

## Solution Architecture

```
Data Sources (Web logs · Transactions · Reviews · Social Media)
                          │
                          ▼
         ┌────────────────────────────────┐
         │        Ingestion Layer         │
         │   Apache Kafka  +  Apache Flume│  ← Real-time & batch
         └────────────────┬───────────────┘
                          │
                          ▼
         ┌────────────────────────────────┐
         │           Data Lake            │
         │         Hadoop HDFS            │  ← Centralized raw storage
         └────────────────┬───────────────┘
                          │
                          ▼
         ┌────────────────────────────────┐
         │       Processing Layer         │
         │        Apache Spark            │  ← Clean · Transform · Customer 360
         └────────────────┬───────────────┘
                          │
                          ▼
         ┌────────────────────────────────┐
         │        Data Warehouse          │
         │    PostgreSQL (Star Schema)    │  ← Analytics-ready tables
         └────────────────────────────────┘
                          │
                          ▼
         ┌────────────────────────────────┐
         │         Visualization          │
         │      Power BI / Tableau        │  ← Dashboards & insights
         └────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Ingestion | Apache Kafka 3.x | Real-time streaming |
| Data Ingestion | Apache Flume | Batch log collection |
| Data Storage | Hadoop HDFS 3.x | Distributed data lake |
| Data Processing | Apache Spark 3.5.2 | Large-scale transformation |
| Data Warehouse | PostgreSQL 16 | Star schema analytics |
| Security | SSL/TLS 1.3 | Encrypted connections |
| Language | Python 3.x, PySpark, SQL | Implementation |

---

## Project Structure

```
Pharos-Kart/
├── data/
│   └── raw/
│       ├── weblogs/
│       │   └── generate_weblogs.py        # 500,000 web log records
│       ├── transactions/
│       │   └── generate_transactions.py   # 100,000 transaction records
│       ├── reviews/
│       │   └── generate_reviews.py        # 50,000 review records
│       └── social/
│           └── generate_social.py         # 20,000 social media records
├── ingestion/
│   └── kafka/
│       ├── producer.py                    # Streams live events to 4 Kafka topics
│       └── consumer.py                    # Kafka → PostgreSQL live ingestion
├── processing/
│   ├── jobs/
│   │   └── clean.py                       # Spark: validate & clean all datasets
│   ├── customer360/
│   │   └── customer360.py                 # Spark: unified customer profile join
│   └── etl/
│       └── etl.py                         # Spark: build warehouse facts & dimensions
├── warehouse/
│   ├── schema/
│   │   └── schema.sql                     # PostgreSQL star schema DDL
│   └── queries/
│       └── analytics.sql                  # Sample analytical queries
├── security/
├── config/
├── logs/
├── docs/
├── .gitignore
└── README.md
```

---

## Setup & Installation

### Prerequisites

- Ubuntu 24.04
- Java 11 (`sudo apt install openjdk-11-jdk`)
- Hadoop 3.x
- Apache Spark 3.5.2
- Apache Kafka 3.x
- PostgreSQL 16
- Python 3.x

### 1. Environment Variables

```bash
export HADOOP_HOME=~/hadoop
export SPARK_HOME=~/spark
export KAFKA_HOME=~/kafka
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$KAFKA_HOME/bin
```

Add these to `~/.bashrc` to make permanent.

### 2. Start Services

```bash
# Hadoop HDFS
start-dfs.sh

# Zookeeper
zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5

# Kafka
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
```

### 3. Install Python Dependencies

```bash
pip install faker pandas kafka-python psycopg2-binary pyspark
```

### 4. Generate Synthetic Data

```bash
python data/raw/weblogs/generate_weblogs.py
python data/raw/transactions/generate_transactions.py
python data/raw/reviews/generate_reviews.py
python data/raw/social/generate_social.py
```

### 5. Load Data into HDFS

```bash
hdfs dfs -mkdir -p /pharoskart/raw/{weblogs,transactions,reviews,social}
hdfs dfs -mkdir -p /pharoskart/{processed,output}

hdfs dfs -put data/raw/weblogs/weblogs.json /pharoskart/raw/weblogs/
hdfs dfs -put data/raw/transactions/transactions.csv /pharoskart/raw/transactions/
hdfs dfs -put data/raw/reviews/reviews.json /pharoskart/raw/reviews/
hdfs dfs -put data/raw/social/social.json /pharoskart/raw/social/
```

### 6. Run Spark Jobs (in order)

```bash
# Step 1: Clean all datasets
spark-submit processing/jobs/clean.py

# Step 2: Build Customer 360 unified profiles
spark-submit processing/customer360/customer360.py

# Step 3: ETL to warehouse tables
spark-submit processing/etl/etl.py
```

### 7. Setup PostgreSQL Warehouse

```bash
# Create database and user
sudo -u postgres psql << 'SQL'
CREATE DATABASE pharoskart;
CREATE USER pharos_user WITH PASSWORD 'pharos123';
GRANT ALL PRIVILEGES ON DATABASE pharoskart TO pharos_user;
\c pharoskart
GRANT ALL ON SCHEMA public TO pharos_user;
SQL

# Create tables
psql -h localhost -U pharos_user -d pharoskart -f warehouse/schema/schema.sql

# Pull output from HDFS
hdfs dfs -getmerge /pharoskart/output/customer360_csv /tmp/customer360.csv
hdfs dfs -getmerge /pharoskart/output/fact_sales_by_category /tmp/fact_sales.csv
hdfs dfs -getmerge /pharoskart/output/fact_product_performance /tmp/fact_products.csv
hdfs dfs -getmerge /pharoskart/output/dim_payment_methods /tmp/dim_payment.csv
hdfs dfs -getmerge /pharoskart/output/dim_traffic_by_device /tmp/dim_device.csv

# Load into PostgreSQL
psql -h localhost -U pharos_user -d pharoskart << 'SQL'
\COPY dim_customers FROM '/tmp/customer360.csv' CSV HEADER;
\COPY fact_sales_by_category FROM '/tmp/fact_sales.csv' CSV HEADER;
\COPY fact_product_performance FROM '/tmp/fact_products.csv' CSV HEADER;
\COPY dim_payment_methods FROM '/tmp/dim_payment.csv' CSV HEADER;
\COPY dim_traffic_by_device FROM '/tmp/dim_device.csv' CSV HEADER;
SQL
```

### 8. Run Kafka Streaming

```bash
# Create topics
kafka-topics.sh --create --topic pharoskart-weblogs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic pharoskart-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic pharoskart-reviews --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic pharoskart-social --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Terminal 1: Start producer
python ingestion/kafka/producer.py

# Terminal 2: Start consumer (streams into PostgreSQL)
python ingestion/kafka/consumer.py
```

---

## Results

### Synthetic Dataset

| Source | Records | File Size | Format |
|---|---|---|---|
| Web logs | 500,000 | 157 MB | JSON (NDJSON) |
| Transactions | 100,000 | 18 MB | CSV |
| Reviews | 50,000 | 14 MB | JSON (NDJSON) |
| Social media | 20,000 | 10 MB | JSON (NDJSON) |
| **Total** | **670,000** | **~199 MB** | |

### HDFS Storage

| Path | Size |
|---|---|
| `/pharoskart/raw/weblogs` | 156.3 MB |
| `/pharoskart/raw/transactions` | 17.8 MB |
| `/pharoskart/raw/reviews` | 13.6 MB |
| `/pharoskart/raw/social` | 10.0 MB |
| `/pharoskart/processed/` | ~45 MB (Parquet compressed) |

### Customer 360

- **10,000 unified customer profiles** built by joining all 4 sources on `customer_id`
- Each profile contains 17 attributes spanning web behaviour, transactions, reviews and social engagement

### Warehouse Tables

| Table | Rows | Type |
|---|---|---|
| `dim_customers` | 10,000 | Dimension |
| `fact_product_performance` | 5,000 | Fact |
| `fact_sales_by_category` | 8 | Fact |
| `dim_payment_methods` | 6 | Dimension |
| `dim_traffic_by_device` | 3 | Dimension |

### Real-Time Streaming

- ~10 records per topic every 5 seconds through Kafka → PostgreSQL
- 4 live tables updated continuously: `live_transactions`, `live_weblogs`, `live_reviews`, `live_social`

### Sample Query Results

```
Top Revenue Categories:
  Home     → ₹30,439,191  (12,645 orders)
  Books    → ₹30,368,702  (12,480 orders)
  Beauty   → ₹30,123,900  (12,517 orders)

Most Used Payment Methods:
  COD          → 16,875 transactions
  Wallet       → 16,717 transactions
  Credit Card  → 16,713 transactions

Device Traffic:
  Tablet   → 167,525 sessions  (avg 1524ms)
  Mobile   → 166,249 sessions  (avg 1523ms)
  Desktop  → 166,226 sessions  (avg 1527ms)
```

---

## Solutions to Challenges

| Challenge | Solution Implemented |
|---|---|
| Slow reporting | Star schema warehouse with fact/dimension tables — queries return in milliseconds |
| No single customer view | Spark joins all 4 sources on `customer_id` into unified Customer 360 profiles |
| Data engineering complexity | Standardized pipeline: Kafka → HDFS → Spark → PostgreSQL |
| Security & privacy | TLSv1.3 encrypted PostgreSQL, `ON CONFLICT DO NOTHING` deduplication guards |

---

## References

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/3.5.2/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Faker Python Library](https://faker.readthedocs.io/)
