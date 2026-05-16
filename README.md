# 🏦 Spark Feature Pipeline | Banking ML Features Production

Production-ready пайплайн для построения витрин данных и Feature Store для AI-моделей банка. Транслирует эксперименты Data Science (Pandas/Jupyter) в масштабируемый PySpark-код с загрузкой в GreenPlum/PostgreSQL.

## 🎯 Что решает

- 🔄 **DS → Production**: рефакторинг pandas-скриптов в распределённый PySpark
- 📊 **Feature Engineering**: расчёт скользящих агрегатов, риск-флагов, поведенческих метрик
- 💾 **Industrial Data Marts**: автоматическая загрузка витрин в GreenPlum/Postgres
- 🛡️ **Data Quality**: валидация схем, контроль полноты, обработка дублей
-  **Dockerized**: готов к запуску в CI/CD и k8s

##  Технологический стек

- **Processing:** PySpark 3.5, SQL Window Functions
- **Storage:** PostgreSQL (GreenPlum-compatible), JDBC
- **Orchestration:** Airflow-ready structure
- **Data Quality:** Schema validation, null handling, deduplication
- **Infrastructure:** Docker, Python 3.10

## 📂 Структура проекта
```
spark_feature_pipeline/
├── notebooks/ # Legacy DS code (Pandas)
├── src/ # Production PySpark modules
├── data/ # Sample raw data
├── docker-compose.yml # Local GreenPlum/Postgres
└── requirements.txt # Dependencies
```

## 🚀 Быстрый старт

### 1. **Клонируй репозиторий:**
```bash
git clone https://github.com/Giganmama/spark_feature_pipeline.git
cd spark_feature_pipeline
```

### 2. **Запусти БД (GreenPlum/Postgres):**
```bash
docker-compose up -d
```

### 3. **Установи зависимости:**
```bash
pip install -r requirements.txt
```

### 4. **Запусти пайплайн:**
```bash
python src/spark_transform.py
```

## 📉 DS → Production Transformation

| Этап | Pandas (DS) | PySpark (Production) |
|------|-------------|----------------------|
| Чтение данных | `pd.read_csv()` | `spark.read.csv().schema()` |
| Агрегации | `groupby().agg()` | `Window.partitionBy().orderBy()` |
| Масштабируемость | RAM limit (~10GB) | Distributed cluster (TB+) |
| Интеграция | CSV/Parquet local | JDBC → GreenPlum/Postgres |
| Качество | Manual checks | Automated schema validation |

📊 Метрики витрины
- ⏱ Объём данных: 5M+ транзакций (тест), масштабируется до 1B+  
- 🧮 Фичи: 30+ признаков (rolling avg, frequency, risk flags)  
- DQ покрытие: 100% проверка nulls, дублей, типов  
- ⬆️ Производительность: PySpark local mode ~40 сек vs Pandas ~120 сек на 1M строк
