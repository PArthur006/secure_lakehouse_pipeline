import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# 1. Força o PySpark a usar o Python do VENV
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def processar_bronze():
    print("Iniciando ingestão Camada Bronze (Raw -> Delta)...")
    
    # 2. Configuração do Motor PySpark
    builder = SparkSession.builder \
        .appName("SecureLakehouse_Bronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # 3. Injeção segura das dependências do Delta Lake
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Reduzindo o barulho de logs no terminal
    spark.sparkContext.setLogLevel("ERROR")

    # 4. Definição de Caminhos
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_dir = os.path.join(base_dir, 'data', '00_raw')
    bronze_dir = os.path.join(base_dir, 'data', '01_bronze')

    # 5. Ingestão do CRM
    crm_csv_path = os.path.join(raw_dir, 'crm_customers.csv')
    crm_delta_path = os.path.join(bronze_dir, 'crm_customers')
    
    print("Ingerindo CRM...")
    df_crm = spark.read.csv(crm_csv_path, header=True, inferSchema=True)
    df_crm.write.format("delta").mode("overwrite").save(crm_delta_path)

    # 6. Ingestão do ERP
    erp_csv_path = os.path.join(raw_dir, 'erp_billing.csv')
    erp_delta_path = os.path.join(bronze_dir, 'erp_billing')
    
    print("Ingerindo ERP...")
    df_erp = spark.read.csv(erp_csv_path, header=True, inferSchema=True)
    df_erp.write.format("delta").mode("overwrite").save(erp_delta_path)

    print(f"Sucesso! Dados salvos na Camada Bronze em formato Delta Lake.")
    spark.stop()

if __name__ == "__main__":
    processar_bronze()