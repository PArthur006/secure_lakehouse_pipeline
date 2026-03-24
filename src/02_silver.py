import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, regexp_replace, when
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from security_utils import hash_sensivel, mascarar_email, mascarar_telefone

# Força o PySpark a usar o Python do VENV
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Carrega o Salt de segurança do arquivo .env
load_dotenv()
SECRET_SALT = os.getenv("LGPD_SALT_KEY")

if not SECRET_SALT:
    raise ValueError("FATAL: Salt de segurança não encontrado. Verifique a variável de ambiente LGPD_SALT_KEY.")

def processar_silver():
    print("Iniciando processamento Camada Silver (Limpeza e LGPD)...")

    builder = SparkSession.builder \
        .appName("SecureLakehouse_Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.dirname(__file__))
    bronze_dir = os.path.join(base_dir, 'data', '01_bronze')
    silver_dir = os.path.join(base_dir, 'data', '02_silver')

    # 1. Leitura dos dados brutos em Delta
    print("Lendo silos da Camada Bronze...")
    df_crm = spark.read.format("delta").load(os.path.join(bronze_dir, 'crm_customers'))
    df_erp = spark.read.format("delta").load(os.path.join(bronze_dir, 'erp_billing'))

    # 2. Qualidade de Dados (Data Quality)
    df_erp_limpo = df_erp.dropna(subset=['MonthlyCharges'])
    print(f"Registors nulos de faturamento removidos. Linhas restantes no ERP: {df_erp_limpo.count()}")

    # 3. Integração
    print("Executando JOIN entre CRM e ERP...")
    df_integrado = df_crm.join(df_erp_limpo, on='customer_id', how='inner')

    # 4. Políticas de Segurança
    print("Aplicando Mascaramento e Salted Hashing em PII via security_utils...")
    df_seguro = df_integrado \
        .withColumn("cpf_anonimizado", hash_sensivel("cpf", SECRET_SALT)) \
        .withColumn("email_mascarado", mascarar_email("email_pessoal")) \
        .withColumn("telefone_mascarado", mascarar_telefone("telefone")) \
        .drop("cpf", "email_pessoal", "telefone", "nome_completo")
    
    # 5. Salvamento na Camada Silver
    silver_path = os.path.join(silver_dir, 'telco_churn_silver')
    df_seguro.write.format("delta").mode("overwrite").save(silver_path)

    print(f"Sucesso! Camada Silver consolidada e anonimizada de acordo com políticas de privacidade.")
    spark.stop()

if __name__ == "__main__":
    processar_silver()