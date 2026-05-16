from pyspark.sql import DataFrame
from config import JDBC_URL, JDBC_PROPS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GreenPlumLoader:
    """Загрузка витрин в GreenPlum/PostgreSQL"""

        def merge_data(self, df: DataFrame, table_name: str, primary_key: str):
        """
        Incremental Load (CDC) logic.
        Inserts new records and updates existing ones based on primary_key.
        """
        logger.info(f"🔄 Running Incremental Merge (Upsert) into {table_name}")
        
        # 1. Load existing data from DB (simplified for local Postgres)
        # In production DWH (Greenplum/Hive), we use MERGE INTO or MERGE USING
        existing_df = self.spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS)
        
        # 2. Find records to UPDATE (exists in both)
        # Using Left Anti Join to find NEW records only (efficient strategy)
        new_records_df = df.join(existing_df, df[primary_key] == existing_df[primary_key], "left_anti")
        
        # 3. Write ONLY new records (Append mode)
        if new_records_df.count() > 0:
            new_records_df.write.jdbc(
                url=JDBC_URL,
                table=table_name,
                mode="append",
                properties=JDBC_PROPS
            )
            logger.info(f"✅ Inserted {new_records_df.count()} new rows")
        else:
            logger.info("ℹ️ No new records to insert")
    
    def __init__(self, spark):
        self.spark = spark
    
    def save_to_table(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """Сохраняет DataFrame в таблицу БД"""
        
        logger.info(f"Saving {df.count()} rows to {table_name}")
        
        df.write.jdbc(
            url=JDBC_URL,
            table=table_name,
            mode=mode,
            properties=JDBC_PROPS
        )
        
        logger.info(f"✅ Successfully saved to {table_name}")
    
    def create_indexes(self, table_name: str, columns: list):
        """Создаёт индексы для ускорения запросов (выполняется отдельно)"""
        # В production это делается через SQL в GreenPlum
        logger.info(f"Creating indexes on {table_name} for columns: {columns}")
