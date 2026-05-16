from pyspark.sql import DataFrame
from config import JDBC_URL, JDBC_PROPS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GreenPlumLoader:
    """Загрузка витрин в GreenPlum/PostgreSQL"""
    
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
