# Databricks notebook source

# Banking Data Engineering Project
print("Welcome to Banking Data Engineering!")

# Check Spark version
print("Spark Version:", spark.version)

# Create a test DataFrame with 5 records
test_df = spark.range(1, 6)

# Display the DataFrame
display(test_df)


# COMMAND ----------


# Bronze layer: Read banking transactions from Unity Catalog Volume

bronze_path = "/Volumes/workspace/default/banking_bronze/bronze_transactions.csv"

transactions_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(bronze_path)
)

# Display all transactions
display(transactions_df)


# COMMAND ----------


# Inspect column names and data types
transactions_df.printSchema()

# Count total records
print("Total transaction records:", transactions_df.count())


# COMMAND ----------


from pyspark.sql import functions as F

# Check missing values in each column
print("Missing values by column:")

transactions_df.select([
    F.count(
        F.when(
            F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
            c
        )
    ).alias(c)
    for c in transactions_df.columns
]).show()

# Check duplicate transaction IDs
duplicate_count = (
    transactions_df.count()
    - transactions_df.dropDuplicates(["transaction_id"]).count()
)

print("Duplicate transaction IDs:", duplicate_count)

# Check transaction status distribution
transactions_df.groupBy("status").count().show()

# Check transaction type distribution
transactions_df.groupBy("transaction_type").count().show()


# COMMAND ----------

# Reload Bronze transactions

bronze_path = "/Volumes/workspace/default/banking_bronze/bronze_transactions.csv"

transactions_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(bronze_path)
)

print("Total transaction records:", transactions_df.count())

display(transactions_df)

# COMMAND ----------


from pyspark.sql import functions as F

# Check missing values in each column
print("Missing values by column:")

transactions_df.select([
    F.count(
        F.when(
            F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
            c
        )
    ).alias(c)
    for c in transactions_df.columns
]).show()

# Check duplicate transaction IDs
duplicate_count = (
    transactions_df.count()
    - transactions_df.dropDuplicates(["transaction_id"]).count()
)

print("Duplicate transaction IDs:", duplicate_count)

# Check transaction status distribution
transactions_df.groupBy("status").count().show()

# Check transaction type distribution
transactions_df.groupBy("transaction_type").count().show()


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# 1. Standardize and enforce data types
standardized_df = (
    transactions_df
    .withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type"))))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("transaction_date", F.to_date("transaction_date"))
    .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
)

# 2. Remove duplicate transaction IDs
deduplicated_df = standardized_df.dropDuplicates(["transaction_id"])

# 3. Define banking validation rules
valid_condition = (
    F.col("transaction_id").isNotNull() &
    (F.col("transaction_id") != "") &
    F.col("account_id").isNotNull() &
    (F.col("account_id") != "") &
    F.col("customer_id").isNotNull() &
    (F.col("customer_id") != "") &
    F.col("transaction_date").isNotNull() &
    F.col("amount").isNotNull() &
    (F.col("amount") > 0) &
    F.col("currency").isin("ZAR") &
    F.col("transaction_type").isin("DEBIT", "CREDIT", "TRANSFER") &
    F.col("status").isin("SUCCESS", "PENDING", "FAILED")
)

# 4. Separate valid and invalid records
silver_df = deduplicated_df.filter(valid_condition)

rejected_df = deduplicated_df.filter(~valid_condition)

# 5. Display results
print("Silver records:", silver_df.count())
print("Rejected records:", rejected_df.count())

display(silver_df)


# COMMAND ----------


# Save cleaned banking transactions as a Delta table

silver_table_path = "/Volumes/workspace/default/banking_bronze/silver_transactions"

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_table_path)

print("Silver Delta data saved successfully!")


# COMMAND ----------


from pyspark.sql import functions as F

# Location of our saved Silver Delta data
silver_table_path = "/Volumes/workspace/default/banking_bronze/silver_transactions"

# Read the saved Delta data
silver_read_df = (
    spark.read
    .format("delta")
    .load(silver_table_path)
)

# Verify record count
print("Silver record count:", silver_read_df.count())

# Display saved records
display(silver_read_df)

# Check the schema
silver_read_df.printSchema()


# COMMAND ----------


from pyspark.sql import functions as F

# Gold Layer: Daily Transaction Summary

daily_summary_df = (
    silver_read_df
    .groupBy("transaction_date")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum(
            F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
        ).alias("successful_transactions"),
        F.sum(
            F.when(F.col("status") == "FAILED", 1).otherwise(0)
        ).alias("failed_transactions"),
        F.sum(
            F.when(F.col("status") == "PENDING", 1).otherwise(0)
        ).alias("pending_transactions")
    )
    .orderBy("transaction_date")
)

display(daily_summary_df)


# COMMAND ----------


# Save Gold daily transaction summary

gold_daily_path = "/Volumes/workspace/default/banking_bronze/gold_daily_transactions"

daily_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_daily_path)

print("Gold daily transaction summary saved successfully!")


# COMMAND ----------


# Read Gold Delta data

gold_daily_df = (
    spark.read
    .format("delta")
    .load(gold_daily_path)
)

print("Gold daily records:", gold_daily_df.count())

display(gold_daily_df)


# COMMAND ----------


from pyspark.sql import functions as F

customer_summary_df = (
    silver_read_df
    .groupBy("customer_id")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum(
            F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
        ).alias("successful_transactions"),
        F.sum(
            F.when(F.col("status") == "FAILED", 1).otherwise(0)
        ).alias("failed_transactions"),
        F.sum(
            F.when(F.col("status") == "PENDING", 1).otherwise(0)
        ).alias("pending_transactions")
    )
    .orderBy("customer_id")
)

display(customer_summary_df)


# COMMAND ----------


gold_customer_path = "/Volumes/workspace/default/banking_bronze/gold_customer_transactions"

customer_summary_df.write.format("delta").mode("overwrite").save(gold_customer_path)

print("Gold customer summary saved successfully!")


# COMMAND ----------


gold_customer_path = "/Volumes/workspace/default/banking_bronze/gold_customer_transactions"

gold_customer_df = spark.read.format("delta").load(gold_customer_path)

print("Gold customer records:", gold_customer_df.count())
display(gold_customer_df)


# COMMAND ----------


gold_customer_df.agg(
    F.sum("total_transactions").alias("total_transactions"),
    F.sum("total_transaction_amount").alias("total_amount")
).show()


# COMMAND ----------


# Source totals from Silver
silver_total = silver_read_df.count()
silver_amount = silver_read_df.agg(
    F.sum("amount").alias("total")
).first()["total"]

# Totals from Gold customer summary
gold_customer_total = gold_customer_df.agg(
    F.sum("total_transactions").alias("total")
).first()["total"]

gold_customer_amount = gold_customer_df.agg(
    F.sum("total_transaction_amount").alias("total")
).first()["total"]

# Totals from Gold daily summary
gold_daily_total = gold_daily_df.agg(
    F.sum("total_transactions").alias("total")
).first()["total"]

gold_daily_amount = gold_daily_df.agg(
    F.sum("total_transaction_amount").alias("total")
).first()["total"]

# Reconciliation
checks = [
    ("Customer transaction count", silver_total, gold_customer_total),
    ("Daily transaction count", silver_total, gold_daily_total),
    ("Customer transaction amount", silver_amount, gold_customer_amount),
    ("Daily transaction amount", silver_amount, gold_daily_amount)
]

for check_name, source_value, target_value in checks:
    status = "PASS" if source_value == target_value else "FAIL"
    print(
        f"{check_name}: {status} | "
        f"Silver={source_value} | Gold={target_value}"
    )


# COMMAND ----------


from pyspark.sql import functions as F

# 1. Check missing required values
missing_records = silver_read_df.filter(
    F.col("transaction_id").isNull() |
    F.col("account_id").isNull() |
    F.col("customer_id").isNull() |
    F.col("transaction_date").isNull() |
    F.col("amount").isNull() |
    F.col("status").isNull()
).count()

# 2. Check duplicate transaction IDs
duplicate_records = (
    silver_read_df
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# 3. Check invalid transaction amounts
invalid_amounts = silver_read_df.filter(
    F.col("amount") <= 0
).count()

# 4. Check invalid transaction statuses
invalid_statuses = silver_read_df.filter(
    ~F.col("status").isin("SUCCESS", "FAILED", "PENDING") |
    F.col("status").isNull()
).count()

# Display results
quality_checks = {
    "Missing required values": missing_records,
    "Duplicate transaction IDs": duplicate_records,
    "Invalid transaction amounts": invalid_amounts,
    "Invalid transaction statuses": invalid_statuses
}

for check_name, failed_count in quality_checks.items():
    result = "PASS" if failed_count == 0 else "FAIL"
    print(f"{check_name}: {result} | Failed records: {failed_count}")

# Fail the notebook if any quality check fails
assert all(value == 0 for value in quality_checks.values()), \
    "Data quality validation failed!"

print("All Silver data-quality checks passed!")


# COMMAND ----------


def validate_silver_data(df):

    checks = {
        "Missing transaction IDs": df.filter(
            F.col("transaction_id").isNull() |
            (F.trim(F.col("transaction_id")) == "")
        ).count(),

        "Missing account IDs": df.filter(
            F.col("account_id").isNull() |
            (F.trim(F.col("account_id")) == "")
        ).count(),

        "Duplicate transaction IDs": (
            df.groupBy("transaction_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        ),

        "Invalid amounts": df.filter(
            F.col("amount").isNull() |
            (F.col("amount") <= 0)
        ).count(),

        "Invalid statuses": df.filter(
            ~F.col("status").isin("SUCCESS", "FAILED", "PENDING") |
            F.col("status").isNull()
        ).count()
    }

    print("BANKING DATA QUALITY REPORT")
    print("-" * 40)

    for check, failed_count in checks.items():
        result = "PASS" if failed_count == 0 else "FAIL"
        print(f"{check}: {result} | Failed: {failed_count}")

    if any(count > 0 for count in checks.values()):
        raise ValueError("Silver data quality validation failed!")

    print("-" * 40)
    print("All data quality checks passed!")

    return checks


# COMMAND ----------


quality_report = validate_silver_data(silver_read_df)


# COMMAND ----------


from pyspark.sql.types import (
    StructType, StructField, StringType,
    DateType, DecimalType
)
from datetime import date
from decimal import Decimal

batch_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False)
])

new_batch_data = [
    ("TXN1006", "ACC2001", "CUST001",
     date(2026, 11, 4), "CREDIT", Decimal("2000.00"), "ZAR", "SUCCESS"),

    ("TXN1007", "ACC2003", "CUST003",
     date(2026, 11, 4), "DEBIT", Decimal("300.00"), "ZAR", "SUCCESS"),

    # Replay of an existing transaction
    ("TXN1002", "ACC2002", "CUST002",
     date(2026, 11, 1), "CREDIT", Decimal("5000.00"), "ZAR", "SUCCESS")
]

incremental_batch_df = spark.createDataFrame(
    new_batch_data,
    schema=batch_schema
)

print("Incremental batch records:", incremental_batch_df.count())
display(incremental_batch_df)


# COMMAND ----------


existing_ids = silver_read_df.select("transaction_id").distinct()

new_transactions_df = incremental_batch_df.join(
    existing_ids,
    on="transaction_id",
    how="left_anti"
)

duplicate_transactions_df = incremental_batch_df.join(
    existing_ids,
    on="transaction_id",
    how="inner"
)

print("Incoming records:", incremental_batch_df.count())
print("New transactions:", new_transactions_df.count())
print("Already existing transactions:", duplicate_transactions_df.count())

print("New transaction records:")
display(new_transactions_df)

print("Existing transaction records:")
display(duplicate_transactions_df)


# COMMAND ----------


staging_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/banking_bronze/incremental_staging"
)

print("Staging record count:", staging_df.count())
staging_df.show(truncate=False)


# COMMAND ----------


print("Incremental batch records:", incremental_batch_df.count())
display(incremental_batch_df)


# COMMAND ----------


incremental_staging_path = "/Volumes/workspace/default/banking_bronze/incremental_staging"

(
    incremental_batch_df.write
    .format("delta")
    .mode("overwrite")
    .save(incremental_staging_path)
)

print("Incremental staging saved successfully!")


# COMMAND ----------


staging_df = spark.read.format("delta").load(
    incremental_staging_path
)

print("Staging record count:", staging_df.count())
display(staging_df)


# COMMAND ----------


silver_target_path = "/Volumes/workspace/default/banking_bronze/silver_transactions"

silver_target_df = spark.read.format("delta").load(silver_target_path)

print("Existing Silver records:", silver_target_df.count())


# COMMAND ----------


from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, silver_target_path)

(
    silver_delta.alias("target")
    .merge(
        staging_df.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Incremental Delta MERGE completed successfully!")


# COMMAND ----------


silver_after_merge_df = spark.read.format("delta").load(
    silver_target_path
)

print("Silver records after MERGE:", silver_after_merge_df.count())

print("Duplicate transaction IDs:")
(
    silver_after_merge_df
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Latest Silver transaction records:")
display(
    silver_after_merge_df.orderBy("transaction_id")
)


# COMMAND ----------


silver_delta = DeltaTable.forPath(spark, silver_target_path)

(
    silver_delta.alias("target")
    .merge(
        staging_df.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

silver_after_rerun_df = spark.read.format("delta").load(
    silver_target_path
)

print("Records after rerunning batch:", silver_after_rerun_df.count())

duplicate_count = (
    silver_after_rerun_df
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate transaction IDs:", duplicate_count)

assert silver_after_rerun_df.count() == 7
assert duplicate_count == 0

print("IDEMPOTENCY TEST PASSED!")


# COMMAND ----------


# Use the latest Silver data
silver_current_df = spark.read.format("delta").load(
    silver_target_path
)

# Refresh daily Gold summary
daily_summary_df = (
    silver_current_df
    .groupBy("transaction_date")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
            .alias("successful_transactions"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))
            .alias("failed_transactions"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))
            .alias("pending_transactions")
    )
)

# Refresh customer Gold summary
customer_summary_df = (
    silver_current_df
    .groupBy("customer_id")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
            .alias("successful_transactions"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))
            .alias("failed_transactions"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))
            .alias("pending_transactions")
    )
)

# Save refreshed Gold datasets
daily_summary_df.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/banking_bronze/gold_daily_transactions"
)

customer_summary_df.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/banking_bronze/gold_customer_transactions"
)

print("Both Gold datasets refreshed successfully!")


# COMMAND ----------


# Read refreshed Gold datasets
gold_daily_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/banking_bronze/gold_daily_transactions"
)

gold_customer_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/banking_bronze/gold_customer_transactions"
)

# Calculate totals
print("Silver transaction count:", silver_current_df.count())

print(
    "Gold daily transaction count:",
    gold_daily_df.agg(F.sum("total_transactions")).first()[0]
)

print(
    "Gold customer transaction count:",
    gold_customer_df.agg(F.sum("total_transactions")).first()[0]
)

print(
    "Gold daily amount:",
    gold_daily_df.agg(F.sum("total_transaction_amount")).first()[0]
)

print(
    "Gold customer amount:",
    gold_customer_df.agg(F.sum("total_transaction_amount")).first()[0]
)

display(gold_daily_df.orderBy("transaction_date"))
display(gold_customer_df.orderBy("customer_id"))


# COMMAND ----------


from pyspark.sql import functions as F

audit_data = [
    (
        "BATCH_20261111_001",
        staging_df.count(),
        new_transactions_df.count(),
        duplicate_transactions_df.count(),
        silver_current_df.count(),
        "SUCCESS"
    )
]

audit_columns = [
    "batch_id",
    "incoming_records",
    "inserted_records",
    "duplicate_records_skipped",
    "silver_records_after_merge",
    "processing_status"
]

audit_df = spark.createDataFrame(audit_data, audit_columns)

audit_df = audit_df.withColumn(
    "processed_at",
    F.current_timestamp()
)

display(audit_df)


# COMMAND ----------


audit_path = "/Volumes/workspace/default/banking_bronze/banking_audit_logs"

audit_df.write.format("delta").mode("append").save(audit_path)

print("Banking audit log saved successfully!")


# COMMAND ----------


audit_read_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/banking_bronze/banking_audit_logs"
)

print("Audit log records:", audit_read_df.count())
display(audit_read_df)


# COMMAND ----------


# Banking pipeline parameters

dbutils.widgets.text(
    "batch_id",
    "BATCH_20261111_002",
    "Batch ID"
)

dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/default/banking_bronze/incremental_staging",
    "Source Path"
)

dbutils.widgets.text(
    "target_path",
    "/Volumes/workspace/default/banking_bronze/silver_transactions",
    "Silver Target Path"
)

# Read parameter values
batch_id = dbutils.widgets.get("batch_id")
source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")

print("BANKING PIPELINE CONFIGURATION")
print("--------------------------------")
print("Batch ID:", batch_id)
print("Source Path:", source_path)
print("Target Path:", target_path)


# COMMAND ----------


def read_banking_batch(source_path, batch_id):

    print("Starting banking batch ingestion")
    print("--------------------------------")

    print("Batch ID:", batch_id)
    print("Source:", source_path)

    # Read incoming Delta batch
    batch_df = (
        spark.read
        .format("delta")
        .load(source_path)
    )

    # Count records
    record_count = batch_df.count()

    print("Incoming records:", record_count)

    # Validate batch
    if record_count == 0:
        raise ValueError("Incoming batch is empty!")

    print("Batch ingestion completed successfully!")

    return batch_df


# COMMAND ----------


incoming_df = read_banking_batch(
    source_path,
    batch_id
)

display(incoming_df)


# COMMAND ----------


from delta.tables import DeltaTable

def process_incremental_batch(batch_df, target_path, batch_id):

    print("Starting incremental processing")
    print("--------------------------------")

    target_df = spark.read.format("delta").load(target_path)

    incoming_count = batch_df.count()

    # Identify new transactions
    existing_ids = target_df.select("transaction_id").distinct()

    new_df = batch_df.join(
        existing_ids,
        on="transaction_id",
        how="left_anti"
    )

    new_count = new_df.count()
    skipped_count = incoming_count - new_count

    print("Batch ID:", batch_id)
    print("Incoming records:", incoming_count)
    print("New records:", new_count)
    print("Existing records skipped:", skipped_count)

    # Insert only new transactions
    delta_target = DeltaTable.forPath(spark, target_path)

    (
        delta_target.alias("target")
        .merge(
            new_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    final_count = (
        spark.read.format("delta")
        .load(target_path)
        .count()
    )

    print("Final Silver records:", final_count)
    print("Incremental processing completed!")

    return {
        "batch_id": batch_id,
        "incoming_records": incoming_count,
        "inserted_records": new_count,
        "skipped_records": skipped_count,
        "final_silver_records": final_count
    }


# COMMAND ----------


processing_result = process_incremental_batch(
    batch_df=incoming_df,
    target_path=target_path,
    batch_id=batch_id
)

print("\nBATCH PROCESSING SUMMARY")
print("-----------------------")

for key, value in processing_result.items():
    print(f"{key}: {value}")


# COMMAND ----------


# Create audit record from the processing result
pipeline_audit_df = spark.createDataFrame([
    (
        processing_result["batch_id"],
        processing_result["incoming_records"],
        processing_result["inserted_records"],
        processing_result["skipped_records"],
        processing_result["final_silver_records"],
        "SUCCESS"
    )
], [
    "batch_id",
    "incoming_records",
    "inserted_records",
    "duplicate_records_skipped",
    "silver_records_after_merge",
    "processing_status"
])

pipeline_audit_df = pipeline_audit_df.withColumn(
    "processed_at",
    F.current_timestamp()
)

# Append to the existing audit Delta dataset
pipeline_audit_df.write \
    .format("delta") \
    .mode("append") \
    .save(audit_path)

print("Pipeline execution recorded in audit log!")


# COMMAND ----------


audit_history_df = spark.read.format("delta").load(audit_path)

print("Total audit records:", audit_history_df.count())

display(
    audit_history_df.orderBy(
        F.col("processed_at").desc()
    )
)


# COMMAND ----------


def execute_pipeline_safely(operation, batch_id):

    print("Starting pipeline execution:", batch_id)

    try:
        result = operation()

        print("Pipeline status: SUCCESS")

        return {
            "batch_id": batch_id,
            "status": "SUCCESS",
            "error_message": None,
            "result": result
        }

    except Exception as e:

        error_message = str(e)

        print("Pipeline status: FAILED")
        print("Error:", error_message)

        return {
            "batch_id": batch_id,
            "status": "FAILED",
            "error_message": error_message,
            "result": None
        }


# COMMAND ----------


def sample_success_operation():
    return "Banking batch processed successfully"

execution_result = execute_pipeline_safely(
    operation=sample_success_operation,
    batch_id="BATCH_SUCCESS_TEST_001"
)

print("\nExecution result:")
print(execution_result)


# COMMAND ----------


def sample_failure_operation():
    raise ValueError("Invalid banking transaction data detected")

failure_result = execute_pipeline_safely(
    operation=sample_failure_operation,
    batch_id="BATCH_FAILURE_TEST_001"
)

print("\nFailure result:")
print(failure_result)


# COMMAND ----------


from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

monitoring_schema = StructType([
    StructField("batch_id", StringType(), False),
    StructField("processing_status", StringType(), False),
    StructField("error_message", StringType(), True)
])

monitoring_records = [
    (
        execution_result["batch_id"],
        execution_result["status"],
        execution_result["error_message"]
    ),
    (
        failure_result["batch_id"],
        failure_result["status"],
        failure_result["error_message"]
    )
]

monitoring_df = spark.createDataFrame(
    monitoring_records,
    schema=monitoring_schema
)

monitoring_df = monitoring_df.withColumn(
    "processed_at",
    F.current_timestamp()
)

display(monitoring_df)


# COMMAND ----------


monitoring_path = (
    "/Volumes/workspace/default/banking_bronze/pipeline_monitoring_logs"
)

monitoring_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(monitoring_path)

print("Pipeline monitoring logs saved successfully!")


# COMMAND ----------


monitoring_read_df = spark.read.format("delta").load(
    monitoring_path
)

print("Total monitored executions:", monitoring_read_df.count())

display(
    monitoring_read_df.orderBy(
        F.col("processed_at").desc()
    )
)


# COMMAND ----------


execution_summary_df = (
    monitoring_read_df
    .groupBy("processing_status")
    .count()
    .withColumnRenamed("count", "total_executions")
)

display(execution_summary_df)


# COMMAND ----------



# COMMAND ----------


def run_banking_pipeline(batch_df, batch_id):
    print("BANKING DATA PIPELINE")
    print("=" * 45)

    print("Batch ID:", batch_id)
    print("Incoming records:", batch_df.count())

    # Step 1: Validate incoming records
    print("\nStep 1: Data quality validation")
    validate_silver_data(batch_df)

    # Step 2: Identify new and duplicate records
    print("\nStep 2: Identify new transactions")
    existing_ids = (
        spark.read.format("delta")
        .load(target_path)
        .select("transaction_id")
        .distinct()
    )

    new_df = batch_df.join(
        existing_ids,
        on="transaction_id",
        how="left_anti"
    )

    new_count = new_df.count()
    duplicate_count = batch_df.count() - new_count

    print("New records:", new_count)
    print("Duplicates skipped:", duplicate_count)

    # Step 3: Insert new records into Silver
    print("\nStep 3: Merge into Silver")
    delta_target = DeltaTable.forPath(spark, target_path)

    (
        delta_target.alias("target")
        .merge(
            new_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Step 4: Refresh Gold summaries
    print("\nStep 4: Refresh Gold tables")

    updated_silver_df = (
        spark.read.format("delta")
        .load(target_path)
    )

    # Rebuild daily summary
    updated_daily_df = (
        updated_silver_df
        .groupBy("transaction_date")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_transaction_amount"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_transactions"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
            F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_transactions")
        )
    )

    updated_daily_df.write.format("delta").mode("overwrite").save(
        "/Volumes/workspace/default/banking_bronze/gold_daily_transactions"
    )

    # Rebuild customer summary
    updated_customer_df = (
        updated_silver_df
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_transaction_amount"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_transactions"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
            F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_transactions")
        )
    )

    updated_customer_df.write.format("delta").mode("overwrite").save(
        "/Volumes/workspace/default/banking_bronze/gold_customer_transactions"
    )

    print("\nPipeline completed successfully!")

    return {
        "batch_id": batch_id,
        "incoming_records": batch_df.count(),
        "inserted_records": new_count,
        "duplicates_skipped": duplicate_count,
        "silver_records_after_merge": updated_silver_df.count(),
        "processing_status": "SUCCESS"
    }


# COMMAND ----------



# COMMAND ----------


print("Pipeline function:", callable(run_banking_pipeline))
print("Silver target:", target_path)
print("Current Silver records:", spark.read.format("delta").load(target_path).count())


# COMMAND ----------


from datetime import date
from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DateType, DecimalType
)

test_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False)
])

test_data = [
    ("TXN1008", "ACC2001", "CUST001", date(2026, 11, 5),
     "CREDIT", Decimal("1800.00"), "ZAR", "SUCCESS"),

    ("TXN1009", "ACC2003", "CUST003", date(2026, 11, 5),
     "DEBIT", Decimal("450.00"), "ZAR", "PENDING")
]

test_batch_df = spark.createDataFrame(test_data, test_schema)

print("Test batch records:", test_batch_df.count())
display(test_batch_df)


# COMMAND ----------


test_result = run_banking_pipeline(
    batch_df=test_batch_df,
    batch_id="BATCH_E2E_TEST_001"
)

print("\nFINAL PIPELINE RESULT")
print(test_result)


# COMMAND ----------



# COMMAND ----------


silver_check = spark.read.format("delta").load(target_path)

daily_check = spark.read.format("delta").load(
    "/Volumes/workspace/default/banking_bronze/gold_daily_transactions"
)

customer_check = spark.read.format("delta").load(
    "/Volumes/workspace/default/banking_bronze/gold_customer_transactions"
)

print("SILVER TRANSACTIONS:", silver_check.count())
print("GOLD DAILY TRANSACTIONS:", daily_check.agg(
    F.sum("total_transactions")
).first()[0])
print("GOLD CUSTOMER TRANSACTIONS:", customer_check.agg(
    F.sum("total_transactions")
).first()[0])

print("\nTOTAL SILVER AMOUNT:")
silver_check.agg(F.sum("amount").alias("total_amount")).show()

print("GOLD DAILY AMOUNT:")
daily_check.agg(
    F.sum("total_transaction_amount").alias("total_amount")
).show()

print("GOLD CUSTOMER AMOUNT:")
customer_check.agg(
    F.sum("total_transaction_amount").alias("total_amount")
).show()


# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql import functions as F

audit_path = "/Volumes/workspace/default/banking_bronze/banking_audit_logs"

e2e_audit_df = spark.createDataFrame([
    Row(
        batch_id=test_result["batch_id"],
        incoming_records=test_result["incoming_records"],
        inserted_records=test_result["inserted_records"],
        duplicate_records_skipped=test_result["duplicates_skipped"],
        silver_records_after_merge=test_result["silver_records_after_merge"],
        processing_status=test_result["processing_status"]
    )
]).withColumn("processed_at", F.current_timestamp())

e2e_audit_df.write.format("delta").mode("append").save(audit_path)

print("End-to-end audit record saved successfully!")


# COMMAND ----------


audit_read_df = spark.read.format("delta").load(audit_path)

display(
    audit_read_df
    .filter(F.col("batch_id") == "BATCH_E2E_TEST_001")
)


# COMMAND ----------


monitoring_path = "/Volumes/workspace/default/banking_bronze/pipeline_monitoring_logs"

e2e_monitoring_df = spark.createDataFrame([
    (
        test_result["batch_id"],
        test_result["processing_status"],
        None
    )
], ["batch_id", "processing_status", "error_message"])

e2e_monitoring_df = e2e_monitoring_df.withColumn(
    "processed_at", F.current_timestamp()
)

e2e_monitoring_df.write.format("delta").mode("append").save(
    monitoring_path
)

print("End-to-end monitoring record saved successfully!")


# COMMAND ----------


from pyspark.sql.types import (
    StructType, StructField, StringType
)

monitoring_schema = StructType([
    StructField("batch_id", StringType(), False),
    StructField("processing_status", StringType(), False),
    StructField("error_message", StringType(), True)
])

monitoring_data = [
    (
        test_result["batch_id"],
        test_result["processing_status"],
        None
    )
]

e2e_monitoring_df = spark.createDataFrame(
    monitoring_data,
    schema=monitoring_schema
)

e2e_monitoring_df = e2e_monitoring_df.withColumn(
    "processed_at",
    F.current_timestamp()
)

e2e_monitoring_df.write.format("delta").mode("append").save(
    monitoring_path
)

print("End-to-end monitoring record saved successfully!")


# COMMAND ----------


monitoring_read_df = spark.read.format("delta").load(
    monitoring_path
)

print("Total monitored executions:", monitoring_read_df.count())

display(
    monitoring_read_df
    .filter(F.col("batch_id") == "BATCH_E2E_TEST_001")
)

print("Execution status summary:")
monitoring_read_df.groupBy(
    "processing_status"
).count().show()


# COMMAND ----------


daily_export_path = "/Volumes/workspace/default/banking_bronze/powerbi_daily_export"
customer_export_path = "/Volumes/workspace/default/banking_bronze/powerbi_customer_export"

daily_check.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(daily_export_path)

customer_check.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(customer_export_path)

print("Power BI Gold exports completed!")
