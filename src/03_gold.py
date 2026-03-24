import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from delta import configure_spark_with_delta_pip

# Força o PySpark a usar o Python do VENV
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def processar_gold():
    print("Iniciando processamento Camada Gold (Feature Engineering para ML)...")
    
    builder = SparkSession.builder \
        .appName("SecureLakehouse_Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.dirname(__file__))
    silver_dir = os.path.join(base_dir, 'data', '02_silver')
    gold_dir = os.path.join(base_dir, 'data', '03_gold')

    # 1. Leitura do dado seguro (Silver)
    print("Lendo tabela Delta da Camada Silver...")
    silver_path = os.path.join(silver_dir, 'telco_churn_silver')
    df_silver = spark.read.format("delta").load(silver_path)

    # 2. Preparação para IA (Feature Engineering)
    print("Aplicando Binarização e Tipagem Estrita...")
    df_gold = df_silver \
        .withColumn("churn_label", when(col("Churn") == "Yes", 1).otherwise(0)) \
        .withColumn("is_female", when(col("gender") == "Female", 1).otherwise(0)) \
        .withColumn("has_partner", when(col("Partner") == "Yes", 1).otherwise(0)) \
        .withColumn("has_dependents", when(col("Dependents") == "Yes", 1).otherwise(0)) \
        .withColumn("paperless_billing", when(col("PaperlessBilling") == "Yes", 1).otherwise(0)) \
        .withColumn("monthly_charges_num", col("MonthlyCharges").cast("double")) \
        .withColumn("total_charges_num", col("TotalCharges").cast("double")) \
        .drop("Churn", "gender", "Partner", "Dependents", "PaperlessBilling", "MonthlyCharges", "TotalCharges")

    df_gold = df_gold.fillna(0.0, subset=["total_charges_num"])

    # 3. Salvamento na Camada Gold
    print("Materializando tabela final na Camada Gold...")
    gold_path = os.path.join(gold_dir, 'features_churn_model')
    df_gold.write.format("delta").mode("overwrite").save(gold_path)

    print(f"Sucesso! Pipeline finalizado. Dados prontos para treinamento de Machine Learning.")
    spark.stop()

if __name__ == "__main__":
    processar_gold()