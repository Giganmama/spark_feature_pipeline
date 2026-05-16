"""
LEGACY DS CODE (Pandas)
Это пример кода, который пишет Data Scientist в Jupyter.
Наша задача — переписать это на PySpark для production.
"""
import pandas as pd

def calculate_client_features(transactions_df, clients_df):
    """Ручной расчёт фич на Pandas (не масштабируется)"""
    
    # Агрегация транзакций
    agg = transactions_df.groupby('client_id').agg({
        'amount': ['sum', 'mean', 'std'],
        'transaction_date': ['min', 'max', 'count']
    }).reset_index()
    
    # Ручное создание флага риска
    agg['high_risk'] = agg[('amount', 'sum')] > 100000
    
    # Слияние с профилями клиентов
    result = agg.merge(clients_df, on='client_id', how='left')
    
    return result

# Проблема: работает только на данных, которые влезают в RAM
