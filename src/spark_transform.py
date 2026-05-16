from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from feature_engineering import calculate_transaction_features, add_recency_features
from db_loader import GreenPlumLoader
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Инициализация Spark сессии"""
    return SparkSession.builder \
        .appName("Banking_Feature_Pipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def define_schema():
    """Строгая схема данных (Data Quality)"""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_category", StringType(), True)
    ])

def main():
    # Инициализация
    spark = create_spark_session()
    logger.info("Spark session created")
    
    # Чтение сырых данных с валидацией схемы
    schema = define_schema()
    transactions_df = spark.read.csv("data/raw_transactions.csv", schema=schema, header=True)
    
    # Data Quality checks
    null_count = transactions_df.filter(
        transactions_df.client_id.isNull() | transactions_df.amount.isNull()
    ).count()
    
    if null_count > 0:
        logger.warning(f"Found {null_count} rows with nulls in critical columns")
        transactions_df = transactions_df.dropna(subset=["client_id", "amount"])
    
    # Feature Engineering
    logger.info("Calculating features...")
    features_df = calculate_transaction_features(transactions_df)
    features_df = add_recency_features(features_df)
    
    # Загрузка в GreenPlum
    loader = GreenPlumLoader(spark)
    loader.merge_data(features_df, "client_features_mart", "client_id")
    
    # Метрики
    logger.info(f"✅ Pipeline completed. Total clients processed: {features_df.count()}")
    
    spark.stop()

if __name__ == "__main__":
    main()
