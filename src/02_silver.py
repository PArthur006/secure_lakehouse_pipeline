import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, regexp_replace, when
from delta import configure_spark_with_delta_pip

# Força o PySpark a usar o Python do VENV
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Em produção, esse "Salt" jamais ficaria no código fonte. Ele seria gerenciado por um serviço de vault ou KMS. Estou ciente disso, porém, para fins de projeto, vou deixar aqui, mas em um cenário real, isso é um risco de segurança.
# Usaremos uma variável de ambiente simulada aqui.
SECRET_SALT = "xK9!wQp@2mZ$vL8"

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
    print("Aplicando Mascaramento e Salted Hashing em PII...")
    df_seguro = df_integrado \
        .withColumn(
            "cpf_anonimizado",
            sha2(concat(col("cpf"), lit(SECRET_SALT)), 256)
        ) \
        .withColumn(
            "email_mascarado",
            regexp_replace(col("email_pessoal"), "^(.).*(@.*)$", "$1***$2")
        ) \
        .withColumn(
            "telefone_mascarado",
            regexp_replace(col("telefone"), "\d{4}$", "****")
        ) \
        .drop("cpf", "email_pessoal", "telefone", "nome_completo")
    
    # 5. Salvamento na Camada Silver
    silver_path = os.path.join(silver_dir, 'telco_churn_silver')
    df_seguro.write.format("delta").mode("overwrite").save(silver_path)

    print(f"Sucesso! Camada Silver consolidada e anonimizada de acordo com políticas de privacidade.")
    spark.stop()

if __name__ == "__main__":
    processar_silver()