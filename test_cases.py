"""
Fixed test cases for lineage agent evaluation
"""

TEST_CASES = [
    # === BASIC CASES ===
    {
        "name": "basic_pyspark_filter",
        "code": """
df = spark.table("users")
filtered_df = df.filter(col("age") > 18)
""",
        "target": "filtered_df",
        "expected_sources": ["users"],
        "expected_operations": ["filter"],
    },
    {
        "name": "basic_pandas_filter",
        "code": """
import pandas as pd
df = pd.read_csv("sales.csv")
high_sales = df[df['amount'] > 1000]
""",
        "target": "high_sales",
        "expected_sources": ["sales.csv"],
        "expected_operations": ["filter"],
    },
    # === PANDAS CASES ===
    {
        "name": "pandas_groupby_merge",
        "code": """
import pandas as pd

# Load data
orders = pd.read_csv("orders.csv")
customers = pd.read_csv("customers.csv")
products = pd.read_csv("products.csv")

# Clean and aggregate
clean_orders = orders.dropna(subset=['customer_id', 'product_id'])
order_summary = clean_orders.groupby('customer_id').agg({
    'amount': 'sum',
    'quantity': 'count'
}).reset_index()

# Merge with customer info
customer_orders = order_summary.merge(customers, on='customer_id', how='left')
final_report = customer_orders.merge(products[['product_id', 'category']], 
                                   left_on='main_product', right_on='product_id')
""",
        "target": "final_report",
        "expected_sources": ["orders.csv", "customers.csv", "products.csv"],
        "expected_operations": [
            "read_csv",
            "dropna",
            "groupby",
            "agg_sum",
            "agg_count",
            "reset_index",
            "merge",
        ],
    },
    {
        "name": "pandas_pivot_analysis",
        "code": """
import pandas as pd

# Load transaction data
transactions = pd.read_parquet("data/transactions.parquet")
regions = pd.read_json("regions.json")

# Filter and clean
valid_transactions = transactions.query("amount > 0 and status == 'completed'")

# Create pivot table
monthly_sales = valid_transactions.pivot_table(
    values='amount',
    index='region_id', 
    columns='month',
    aggfunc='sum'
).fillna(0)

# Add region names
enriched_sales = monthly_sales.merge(regions.set_index('region_id'), 
                                   left_index=True, right_index=True)
""",
        "target": "enriched_sales",
        "expected_sources": ["data/transactions.parquet", "regions.json"],
        "expected_operations": [
            "read_parquet",
            "read_json",
            "query",
            "pivot_table",
            "agg_sum",
            "fillna",
            "set_index",
            "merge",
        ],
    },
    # === PYSPARK CASES ===
    {
        "name": "pyspark_complex_pipeline",
        "code": """
from pyspark.sql import functions as F

# Load source tables
transactions = spark.table("warehouse.transactions")
customers = spark.table("warehouse.customers") 
products = spark.table("warehouse.products")

# Data cleaning and filtering
clean_transactions = transactions.filter(
    (F.col("amount") > 0) & 
    (F.col("transaction_date") >= "2023-01-01")
).dropna(subset=["customer_id", "product_id"])

# Aggregations
customer_metrics = clean_transactions.groupBy("customer_id").agg(
    F.sum("amount").alias("total_spent"),
    F.count("*").alias("transaction_count"),
    F.avg("amount").alias("avg_transaction_amount")
)

# Window functions for ranking
window_spec = Window.partitionBy("customer_segment").orderBy(F.desc("total_spent"))
ranked_customers = customer_metrics.join(
    customers.select("customer_id", "customer_name", "customer_segment"), 
    "customer_id"
).withColumn("rank_in_segment", F.row_number().over(window_spec))

# Final filtering and selection
top_customers = ranked_customers.filter(F.col("rank_in_segment") <= 10).select(
    "customer_id", "customer_name", "customer_segment", 
    "total_spent", "transaction_count", "rank_in_segment"
)
""",
        "target": "top_customers",
        "expected_sources": ["warehouse.transactions", "warehouse.customers"],
        "expected_operations": [
            "table",
            "filter",
            "dropna",
            "groupBy",
            "agg_sum",
            "agg_count",
            "agg_avg",
            "alias",
            "join",
            "select",
            "withColumn",
            "window_row_number",
        ],
    },
    {
        "name": "pyspark_union_and_dedupe",
        "code": """
# Load multiple data sources
current_users = spark.table("prod.users")
legacy_users = spark.table("legacy.old_users")
imported_users = spark.read.parquet("s3://bucket/imported_users/")

# Standardize schemas
standardized_current = current_users.select("user_id", "email", "created_date")
standardized_legacy = legacy_users.select("old_id", "email_address", "signup_date")
standardized_imported = imported_users.select("id", "email", "date_joined")

# Union all sources
all_users = standardized_current.unionByName(standardized_legacy) \\
                               .unionByName(standardized_imported)

# Deduplication and cleaning
unique_users = all_users.dropDuplicates(["email"]) \\
                       .filter(col("email").isNotNull()) \\
                       .orderBy("created_date")
""",
        "target": "unique_users",
        "expected_sources": [
            "prod.users",
            "legacy.old_users",
            "s3://bucket/imported_users/",
        ],
        "expected_operations": [
            "table",
            "read_parquet",
            "select",
            "unionByName",
            "dropDuplicates",
            "filter",
            "orderBy",
        ],
    },
    # === SPARK SQL CASES ===
    {
        "name": "spark_sql_cte",
        "code": '''
spark.sql("""
WITH recent_orders AS (
    SELECT customer_id, product_id, amount, order_date
    FROM raw.orders 
    WHERE order_date >= '2023-01-01'
),
customer_totals AS (
    SELECT 
        customer_id,
        SUM(amount) as total_amount,
        COUNT(*) as order_count
    FROM recent_orders
    GROUP BY customer_id
)
SELECT 
    c.customer_name,
    ct.total_amount,
    ct.order_count,
    c.customer_segment
FROM customer_totals ct
JOIN dim.customers c ON ct.customer_id = c.customer_id
WHERE ct.total_amount > 5000
""").createOrReplaceTempView("high_value_customers")

final_analysis = spark.table("high_value_customers")
''',
        "target": "final_analysis",
        "expected_sources": ["raw.orders", "dim.customers"],
        "expected_operations": [
            "sql",
            "CTE",
            "SELECT",
            "WHERE",
            "GROUP BY",
            "agg_SUM",
            "agg_COUNT",
            "JOIN",
            "createOrReplaceTempView",
            "table",
        ],
    },
    {
        "name": "spark_sql_window_functions",
        "code": '''
# Create a complex analytical view
spark.sql("""
SELECT 
    product_id,
    category,
    monthly_sales,
    LAG(monthly_sales, 1) OVER (PARTITION BY product_id ORDER BY month) as prev_month_sales,
    SUM(monthly_sales) OVER (PARTITION BY category ORDER BY month 
                            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3month_sales,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY monthly_sales DESC) as sales_rank
FROM (
    SELECT 
        product_id,
        category,
        DATE_TRUNC('month', sale_date) as month,
        SUM(amount) as monthly_sales
    FROM fact.sales s
    JOIN dim.products p ON s.product_id = p.product_id
    WHERE sale_date >= '2023-01-01'
    GROUP BY product_id, category, DATE_TRUNC('month', sale_date)
) monthly_summary
""").createOrReplaceTempView("product_analytics")

product_insights = spark.table("product_analytics")
''',
        "target": "product_insights",
        "expected_sources": ["fact.sales", "dim.products"],
        "expected_operations": [
            "sql",
            "SELECT",
            "JOIN",
            "WHERE",
            "GROUP BY",
            "agg_SUM",
            "window_LAG",
            "window_SUM",
            "window_ROW_NUMBER",
            "createOrReplaceTempView",
            "table",
        ],
    },
    # === MIXED CASES ===
    {
        "name": "mixed_pandas_to_spark",
        "code": """
import pandas as pd

# Start with pandas for small data processing
lookup_df = pd.read_excel("reference/product_mapping.xlsx")
category_mapping = lookup_df.set_index('old_product_id')['new_category'].to_dict()

# Convert to Spark for big data processing
spark_lookup = spark.createDataFrame(lookup_df)

# Main data processing in Spark
large_dataset = spark.table("warehouse.large_sales_table")
mapped_data = large_dataset.join(spark_lookup, 
                                large_dataset.product_id == spark_lookup.old_product_id)

# Final aggregation
category_summary = mapped_data.groupBy("new_category").agg(
    sum("sales_amount").alias("total_sales"),
    count("*").alias("record_count")
)
""",
        "target": "category_summary",
        "expected_sources": [
            "reference/product_mapping.xlsx",
            "warehouse.large_sales_table",
        ],
        "expected_operations": [
            "read_excel",
            "set_index",
            "to_dict",
            "createDataFrame",
            "table",
            "join",
            "groupBy",
            "agg_sum",
            "agg_count",
            "alias",
        ],
    },
]

# === SPECIFIC AGGREGATION TEST CASES ===
AGGREGATION_TEST_CASES = [
    {
        "name": "detailed_spark_aggregations",
        "code": """
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load data
transactions = spark.table("fact.transactions")
products = spark.table("dim.products")

# Basic aggregations by category
category_stats = transactions.join(products, "product_id").groupBy("category").agg(
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("avg_transaction"), 
    F.count("*").alias("transaction_count"),
    F.min("amount").alias("min_amount"),
    F.max("amount").alias("max_amount"),
    F.stddev("amount").alias("amount_stddev"),
    F.countDistinct("customer_id").alias("unique_customers")
)

# Window function aggregations
window_spec = Window.partitionBy("category").orderBy(F.desc("total_revenue"))
ranked_categories = category_stats.withColumn(
    "revenue_rank", F.row_number().over(window_spec)
).withColumn(
    "revenue_percentile", F.percent_rank().over(window_spec)
)

# Final filtering
top_categories = ranked_categories.filter(F.col("revenue_rank") <= 5)
""",
        "target": "top_categories",
        "expected_sources": ["fact.transactions", "dim.products"],
        "expected_operations": [
            "table",
            "join",
            "groupBy",
            "agg_sum",
            "agg_avg",
            "agg_count",
            "agg_min",
            "agg_max",
            "agg_stddev",
            "agg_countDistinct",
            "alias",
            "withColumn",
            "window_row_number",
            "window_percent_rank",
            "filter",
        ],
    },
    {
        "name": "complex_pandas_aggregations",
        "code": """
import pandas as pd
import numpy as np

# Load data
sales = pd.read_csv("sales_data.csv")
products = pd.read_csv("products.csv")

# Merge data
sales_with_products = sales.merge(products, on="product_id")

# Complex aggregations
category_analysis = sales_with_products.groupby("category").agg({
    "amount": ["sum", "mean", "median", "std", "min", "max"],
    "quantity": ["sum", "mean"],
    "customer_id": "nunique",
    "order_date": ["min", "max"]
}).round(2)

# Flatten multi-level columns
category_analysis.columns = [
    "total_amount", "avg_amount", "median_amount", "std_amount", "min_amount", "max_amount",
    "total_quantity", "avg_quantity", "unique_customers", "first_order", "last_order"
]

# Add calculated fields
category_analysis["coefficient_of_variation"] = (
    category_analysis["std_amount"] / category_analysis["avg_amount"]
)

final_analysis = category_analysis.reset_index().sort_values("total_amount", ascending=False)
""",
        "target": "final_analysis",
        "expected_sources": ["sales_data.csv", "products.csv"],
        "expected_operations": [
            "read_csv",
            "merge",
            "groupby",
            "agg_sum",
            "agg_mean",
            "agg_median",
            "agg_std",
            "agg_min",
            "agg_max",
            "agg_nunique",
            "round",
            "reset_index",
            "sort_values",
        ],
    },
    {
        "name": "sql_aggregation_functions",
        "code": '''
# Complex SQL with multiple aggregation types
spark.sql("""
SELECT 
    category,
    product_subcategory,
    
    -- Basic aggregations
    SUM(sales_amount) as total_sales,
    AVG(sales_amount) as avg_sales,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    
    -- Statistical aggregations  
    MIN(sales_amount) as min_sale,
    MAX(sales_amount) as max_sale,
    STDDEV(sales_amount) as sales_stddev,
    VARIANCE(sales_amount) as sales_variance,
    
    -- Percentile aggregations
    PERCENTILE_APPROX(sales_amount, 0.5) as median_sale,
    PERCENTILE_APPROX(sales_amount, 0.95) as p95_sale,
    
    -- Window aggregations
    SUM(sales_amount) OVER (PARTITION BY category) as category_total,
    RANK() OVER (PARTITION BY category ORDER BY SUM(sales_amount) DESC) as subcategory_rank,
    
    -- Date aggregations
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction
    
FROM fact.sales s
JOIN dim.products p ON s.product_id = p.product_id  
WHERE transaction_date >= '2023-01-01'
GROUP BY category, product_subcategory
HAVING SUM(sales_amount) > 10000
ORDER BY total_sales DESC
""").createOrReplaceTempView("detailed_sales_analysis")

final_report = spark.table("detailed_sales_analysis")
''',
        "target": "final_report",
        "expected_sources": ["fact.sales", "dim.products"],
        "expected_operations": [
            "sql",
            "SELECT",
            "JOIN",
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "agg_SUM",
            "agg_AVG",
            "agg_COUNT",
            "agg_COUNT_DISTINCT",
            "agg_MIN",
            "agg_MAX",
            "agg_STDDEV",
            "agg_VARIANCE",
            "agg_PERCENTILE_APPROX",
            "window_SUM",
            "window_RANK",
            "createOrReplaceTempView",
            "table",
        ],
    },
]

# === EQUIVALENT TEST CASES ===
EQUIVALENT_TEST_CASES = [
    {
        "scenario": "basic_filter_and_group",
        "pandas": {
            "name": "pandas_filter_group",
            "code": """
import pandas as pd

# Load data
sales = pd.read_csv("sales_data.csv")
customers = pd.read_csv("customers.csv")

# Filter recent sales
recent_sales = sales[sales['date'] >= '2023-01-01']

# Group by customer
customer_totals = recent_sales.groupby('customer_id')['amount'].sum().reset_index()

# Join with customer info
final_result = customer_totals.merge(customers, on='customer_id')
""",
            "target": "final_result",
            "expected_sources": ["sales_data.csv", "customers.csv"],
            "expected_operations": [
                "read_csv",
                "filter",
                "groupby",
                "agg_sum",
                "reset_index",
                "merge",
            ],
        },
        "pyspark": {
            "name": "pyspark_filter_group",
            "code": """
from pyspark.sql import functions as F

# Load data
sales = spark.table("sales_data")
customers = spark.table("customers")

# Filter recent sales
recent_sales = sales.filter(F.col('date') >= '2023-01-01')

# Group by customer
customer_totals = recent_sales.groupBy('customer_id').agg(F.sum('amount').alias('amount'))

# Join with customer info
final_result = customer_totals.join(customers, 'customer_id')
""",
            "target": "final_result",
            "expected_sources": ["sales_data", "customers"],
            "expected_operations": [
                "table",
                "filter",
                "groupBy",
                "agg_sum",
                "alias",
                "join",
            ],
        },
    },
    {
        "scenario": "aggregation_with_multiple_metrics",
        "pandas": {
            "name": "pandas_multi_agg",
            "code": """
import pandas as pd

# Load transaction data
transactions = pd.read_csv("transactions.csv")

# Filter valid transactions
clean_tx = transactions[(transactions['amount'] > 0) & (transactions['status'] == 'completed')]

# Multi-level aggregation
customer_stats = clean_tx.groupby('customer_id').agg({
    'amount': ['sum', 'mean', 'count'],
    'transaction_date': 'max'
}).round(2)

# Flatten column names
customer_stats.columns = ['total_amount', 'avg_amount', 'num_transactions', 'last_transaction']
final_stats = customer_stats.reset_index()
""",
            "target": "final_stats",
            "expected_sources": ["transactions.csv"],
            "expected_operations": [
                "read_csv",
                "filter",
                "groupby",
                "agg_sum",
                "agg_mean",
                "agg_count",
                "agg_max",
                "round",
                "reset_index",
            ],
        },
        "pyspark": {
            "name": "pyspark_multi_agg",
            "code": """
from pyspark.sql import functions as F

# Load transaction data
transactions = spark.table("transactions")

# Filter valid transactions  
clean_tx = transactions.filter((F.col('amount') > 0) & (F.col('status') == 'completed'))

# Multi-level aggregation
final_stats = clean_tx.groupBy('customer_id').agg(
    F.sum('amount').alias('total_amount'),
    F.avg('amount').alias('avg_amount'), 
    F.count('amount').alias('num_transactions'),
    F.max('transaction_date').alias('last_transaction')
).select(
    'customer_id', 
    F.round('total_amount', 2).alias('total_amount'),
    F.round('avg_amount', 2).alias('avg_amount'),
    'num_transactions',
    'last_transaction'
)
""",
            "target": "final_stats",
            "expected_sources": ["transactions"],
            "expected_operations": [
                "table",
                "filter",
                "groupBy",
                "agg_sum",
                "agg_avg",
                "agg_count",
                "agg_max",
                "alias",
                "select",
                "round",
            ],
        },
    },
    {
        "scenario": "pivot_and_reshape",
        "pandas": {
            "name": "pandas_pivot",
            "code": """
import pandas as pd

# Load sales data
monthly_sales = pd.read_csv("monthly_sales.csv")

# Create pivot table
sales_pivot = monthly_sales.pivot_table(
    values='revenue',
    index='product_id', 
    columns='month',
    aggfunc='sum'
).fillna(0)

# Add totals
sales_pivot['total'] = sales_pivot.sum(axis=1)
final_pivot = sales_pivot.reset_index()
""",
            "target": "final_pivot",
            "expected_sources": ["monthly_sales.csv"],
            "expected_operations": [
                "read_csv",
                "pivot_table",
                "agg_sum",
                "fillna",
                "sum",
                "reset_index",
            ],
        },
        "pyspark": {
            "name": "pyspark_pivot",
            "code": """
from pyspark.sql import functions as F

# Load sales data
monthly_sales = spark.table("monthly_sales")

# Create pivot (PySpark approach)
sales_pivot = monthly_sales.groupBy('product_id').pivot('month').agg(F.sum('revenue')).fillna(0)

# Add totals (assuming months Jan, Feb, Mar for example)
final_pivot = sales_pivot.withColumn('total', 
    F.coalesce(F.col('Jan'), F.lit(0)) + 
    F.coalesce(F.col('Feb'), F.lit(0)) + 
    F.coalesce(F.col('Mar'), F.lit(0))
)
""",
            "target": "final_pivot",
            "expected_sources": ["monthly_sales"],
            "expected_operations": [
                "table",
                "groupBy",
                "pivot",
                "agg_sum",
                "fillna",
                "withColumn",
                "coalesce",
            ],
        },
    },
]
