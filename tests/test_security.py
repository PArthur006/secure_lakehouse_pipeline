import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row

# 1. Configuração de Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from security_utils import hash_sensivel, mascarar_email, mascarar_telefone

# 2. Cria um cluster Spark local de 1 núcleo apenas uma vez para todos os testes
@pytest.fixture(scope="session")
def spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark_session = SparkSession.builder \
        .appName("Teste_Automacao_Seguranca") \
        .master("local[1]") \
        .getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()

# 3. Casos de Teste

def test_hash_sensivel_irreversivel(spark):
    """Garante que o CPF seja mascarado em um hash SHA-256 de exatos 64 caracteres alfanuméricos."""
    cpf_real = "123.456.789-00"
    salt = "CHAVE_TESTE_123"
    
    df = spark.createDataFrame([Row(cpf=cpf_real)])
    
    df_resultado = df.withColumn("cpf_hash", hash_sensivel("cpf", salt))
    resultado = df_resultado.collect()[0]["cpf_hash"]
    
    # Se algo aqui for Falso, o pipeline de deploy quebra
    assert resultado != cpf_real, "FALHA: O CPF original está sendo exposto."
    assert len(resultado) == 64, "FALHA: O Hash gerado não possui 64 caracteres padrão do SHA-256."

def test_mascarar_email_regra_negocio(spark):
    """Garante que o email exponha apenas a primeira letra e o domínio."""
    email_real = "pedro.arthur@unb.br"
    
    df = spark.createDataFrame([Row(email=email_real)])
    df_resultado = df.withColumn("email_mask", mascarar_email("email"))
    resultado = df_resultado.collect()[0]["email_mask"]
    
    assert resultado == "p***@unb.br", f"FALHA: A máscara gerou o formato incorreto: {resultado}"

def test_mascarar_telefone_regra_negocio(spark):
    """Garante que os 4 últimos dígitos do telefone sejam ofuscados."""
    telefone_real = "+55 61 98765-4321"
    
    df = spark.createDataFrame([Row(telefone=telefone_real)])
    df_resultado = df.withColumn("tel_mask", mascarar_telefone("telefone"))
    resultado = df_resultado.collect()[0]["tel_mask"]
    
    assert resultado == "+55 61 98765-****", f"FALHA: A máscara de telefone falhou: {resultado}"