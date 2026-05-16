from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_transaction_features(df):
    """Production Feature Engineering на PySpark"""
    
    # Оконные функции для расчёта агрегатов
    window_spec = Window.partitionBy("client_id").orderBy("transaction_date")
    
    features_df = df.groupBy("client_id").agg(
        # Суммарные метрики
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.stddev("amount").alias("std_amount"),
        
        # Временные метрики
        F.min("transaction_date").alias("first_transaction"),
        F.max("transaction_date").alias("last_transaction"),
        F.count("*").alias("transaction_count"),
        
        # Risk flags
        F.max(F.when(F.col("amount") > 100000, 1).otherwise(0)).alias("has_large_transaction"),
        F.avg(F.when(F.col("transaction_type") == "ATM_WITHDRAWAL", 1).otherwise(0)).alias("atm_usage_ratio")
    )
    
    return features_df

def add_recency_features(df):
    """Добавляет recency/frequency метрики"""
    
    df = df.withColumn(
        "days_since_last_transaction",
        F.datediff(F.current_date(), F.col("last_transaction"))
    )
    
    df = df.withColumn(
        "avg_days_between_transactions",
        F.col("days_since_last_transaction") / F.greatest(F.col("transaction_count") - 1, F.lit(1))
    )
    
    return df
