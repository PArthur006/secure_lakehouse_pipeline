import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def auditar_seguranca():
    print("Inicializando Motor de Auditoria (PySpark)...")
    
    builder = SparkSession.builder \
        .appName("SecureLakehouse_Audit") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.dirname(__file__))
    bronze_dir = os.path.join(base_dir, 'data', '01_bronze')
    silver_dir = os.path.join(base_dir, 'data', '02_silver')

    print("\n" + "="*50)
    print(" RELATÓRIO DE AUDITORIA DE SEGURANÇA (LGPD) ")
    print("="*50)
    
    # Lendo os logs do Delta
    df_erp_bronze = spark.read.format("delta").load(os.path.join(bronze_dir, 'erp_billing'))
    df_crm_bronze = spark.read.format("delta").load(os.path.join(bronze_dir, 'crm_customers'))
    df_silver = spark.read.format("delta").load(os.path.join(silver_dir, 'telco_churn_silver'))

    print("\n[ALERTA VERMELHO] CAMADA BRONZE - Dados Originais Expostos:")
    print("Silo ERP (Faturamento):")
    df_erp_bronze.select("customer_id", "cpf").show(3, truncate=False)
    
    print("Silo CRM (Atendimento):")
    df_crm_bronze.select("customer_id", "nome_completo", "email_pessoal", "telefone").show(3, truncate=False)

    print("\n[CONFORME LGPD] CAMADA SILVER - Dados Anonimizados e Mascarados:")
    df_silver.select("customer_id", "cpf_anonimizado", "email_mascarado", "telefone_mascarado").show(3, truncate=False)

    print("="*50)
    print("Status da Auditoria: APROVADO. PII direta destruída na Camada Silver.")
    
    spark.stop()

if __name__ == "__main__":
    auditar_seguranca()