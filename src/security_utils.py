from pyspark.sql.functions import col, sha2, concat, lit, regexp_replace

def hash_sensivel(nome_coluna, salt):
    """
    Aplica Salted Hashing SHA-256 em uma coluna.
    Retorna uma expressão de Coluna do PySpark.
    """
    return sha2(concat(col(nome_coluna), lit(salt)), 256)

def mascarar_email(nome_coluna):
    """
    Mascara o email mantendo a primeira letra e o domínio. (Ex: p***@gmail.com)
    """
    return regexp_replace(col(nome_coluna), "^(.).*(@.*)$", "$1***$2")

def mascarar_telefone(nome_coluna):
    """
    Mascara os últimos 4 dígitos do telefone. (Ex: +55 11 9999-****)
    """
    return regexp_replace(col(nome_coluna), r"\d{4}$", "****")