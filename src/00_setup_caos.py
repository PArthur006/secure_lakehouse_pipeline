import os
import pandas as pd
import numpy as np
from faker import Faker

def gerar_caos_integrado():
    fake = Faker('pt_BR')
    Faker.seed(42)
    np.random.seed(42)

    raw_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', '00_raw')
    kaggle_file = os.path.join(raw_path, 'WA_FN-UseC_-Telco-Customer-Churn.csv')

    if not os.path.exists(kaggle_file):
        print(f"ERRO: Arquivo {kaggle_file} não encontrado na pasta 00_raw.")
        return

    # Lendo o dataset real
    df_base = pd.read_csv(kaggle_file)
    num_registros = len(df_base)
    print(f"Lendo {num_registros} registros do Kaggle. Injetando PII falso...")

    # Injetando Identidade 
    df_base['nome_completo'] = [fake.name() for _ in range(num_registros)]
    df_base['cpf'] = [fake.unique.cpf() for _ in range(num_registros)]
    df_base['email_pessoal'] = [fake.unique.email() for _ in range(num_registros)]
    df_base['telefone'] = [fake.cellphone_number() for _ in range(num_registros)]

    # Silo 1: CRM (Focado em atendimento e retenção. Tem Nome, Email e Churn)
    cols_crm = ['customerID', 'nome_completo', 'email_pessoal', 'telefone', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'InternetService', 'TechSupport', 'Contract', 'Churn']
    df_crm = df_base[cols_crm].copy()
    df_crm.rename(columns={'customerID': 'customer_id'}, inplace=True)

    # Silo 2: ERP (Focado em faturamento. Tem o CPF, que é o dado mais restrito, e valores)
    cols_erp = ['customerID', 'cpf', 'PaymentMethod', 'MonthlyCharges', 'TotalCharges', 'PaperlessBilling', 'tenure']
    df_erp = df_base[cols_erp].copy()
    df_erp.rename(columns={'customerID': 'customer_id'}, inplace=True)

    # Simulando perda de pacotes ou falha de sistema (Nulos) no ERP para forçar limpeza na Silver
    indices_sujos = np.random.choice(df_erp.index, size=int(num_registros * 0.03), replace=False)
    df_erp.loc[indices_sujos, 'MonthlyCharges'] = np.nan

    # Salvando os silos corrompidos
    df_crm.to_csv(os.path.join(raw_path, 'crm_customers.csv'), index=False)
    df_erp.to_csv(os.path.join(raw_path, 'erp_billing.csv'), index=False)

    print("Separação de silos concluída! O caos estruturado está pronto.")

if __name__ == '__main__':
    gerar_caos_integrado()