import subprocess
import sys
import os
from prefect import task, flow

# Garante o uso estrito do venv

PYTHON_EXE = sys.executable

env_vars = os.environ.copy()
env_vars['PYSPARK_PYTHON'] = PYTHON_EXE
env_vars['PYSPARK_DRIVER_PYTHON'] = PYTHON_EXE

@task(name="0. Setup de Caos Sintético", log_prints=True)
def task_setup_caos():
    print("Executando geração de dados e separação de silos...")
    subprocess.run([PYTHON_EXE, "src/00_setup_caos.py"], check=True, env=env_vars)

@task(name="1. Ingestão Camada Bronze", log_prints=True)
def task_bronze():
    print("Executando ingestão Raw para Delta Lake...")
    subprocess.run([PYTHON_EXE, "src/01_bronze.py"], check=True, env=env_vars)

@task(name="2. Segurança e LGPD Camada Silver", log_prints=True)
def task_silver():
    print("Aplicando Mascaramento e Salted Hashing...")
    subprocess.run([PYTHON_EXE, "src/02_silver.py"], check=True, env=env_vars)

@task(name="3. Feature Engineering Camada Gold", log_prints=True)
def task_gold():
    print("Executando One-Hot Encoding e Tipagem...")
    subprocess.run([PYTHON_EXE, "src/03_gold.py"], check=True, env=env_vars)

@task(name="4. Auditoria de Conformidade", log_prints=True)
def task_audit():
    print("Gerando relatório de LGPD...")
    subprocess.run([PYTHON_EXE, "src/04_inspect_lgpd.py"], check=True, env=env_vars)

@flow(name="Secure Lakehouse Pipeline - Core", log_prints=True)
def run_secure_lakehouse():
    task_setup_caos()
    task_bronze()
    task_silver()
    task_gold()
    task_audit()

if __name__ == "__main__":
    run_secure_lakehouse()