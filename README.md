# 🛡️ Secure Lakehouse Pipeline: Engenharia de Dados & Adequação LGPD

Este projeto simula a construção de um Data Lakehouse corporativo focado em segurança da informação, privacidade de dados (LGPD) e preparação de _features_ para modelos de Machine Learning (Previsão de Churn).

O objetivo principal não é treinar a IA, mas arquitetar a infraestrutura de dados que permite aos cientistas de dados trabalharem com total conformidade legal, garantindo que Informações Pessoais Identificáveis (PII) sejam criptografadas e isoladas da camada de _Analytics_.

## 🏗️ Arquitetura e Fluxo de Dados

O pipeline foi construído sobre a **Arquitetura Medallion**, utilizando **Apache Spark (PySpark)** para processamento distribuído e **Delta Lake** para garantir transações ACID, _Time Travel_ e confiabilidade na escrita.

1. **Simulação de Caos (Silos Corporativos):** Um _dataset_ de Churn foi enriquecido com PIIs sintéticas (CPFs, Nomes, Emails) via biblioteca `Faker` e deliberadamente fragmentado em dois sistemas inconsistentes: um CRM (Atendimento) e um ERP (Faturamento com dados nulos).
2. **Camada Bronze (Ingestão Raw):** Ingestão bruta dos silos em formato Delta. Preservação do histórico imutável sem transformações.
3. **Camada Silver (Núcleo de Segurança):** Integração dos sistemas (JOIN) e sanitização. É nesta etapa que a governança cibernética é aplicada para destruir a PII direta.
4. **Camada Gold (Machine Learning Features):** Engenharia de _features_ binarizando variáveis categóricas (One-Hot Encoding) e tipagem estrita para consumo direto por algoritmos de Inteligência Artificial.

## 🔐 Engenharia de Segurança Cibernética

Para garantir a total conformidade com a LGPD e mitigar riscos de vazamento (_Data Breach_), a Camada Silver implementa:

- **Salted Hashing (SHA-256):** CPFs não são apenas codificados. Um _Secret Salt_ é concatenado ao dado antes do _hash_, tornando ataques de dicionário (_Rainbow Tables_) matematicamente inviáveis. O cientista de dados consegue rastrear o usuário único pelo Hash, mas nunca descobrirá o CPF real.
- **Data Masking (Mascaramento Dinâmico):** E-mails e telefones são ofuscados usando Expressões Regulares (Regex) nativas do PySpark (`p***@gmail.com`). Permite a validação parcial pelo atendimento ao cliente sem expor a identidade completa no Data Warehouse.
- **Destruição de Colunas Críticas:** Nomes completos e dados originais expostos são fisicamente "dropados" do _DataFrame_ antes da materialização no Delta Lake da Silver.

### 📋 Evidência de Auditoria (PoC)

Abaixo está o registro real gerado pelo motor de auditoria (`src/04_inspect_lgpd.py`), comprovando a transformação criptográfica entre as camadas:



## 🚀 Como Executar Localmente

### 1. Instalação e Configuração

Recomenda-se o uso do Python 3.11 para máxima estabilidade com o Apache Spark.

``` bash
python -m venv venv

# Linux / macOS:
source venv/bin/activate
# Windows
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 2. Execução da Esteira de Dados

``` bash
# 1. Gera os silos sintéticos corrompidos na pasta data/00_raw
python src/00_setup_caos.py

# 2. Ingestão Delta na Camada Bronze
python src/01_bronze.py

# 3. Tratamento de Churn e Criptografia LGPD na Camada Silver
python src/02_silver.py

# 4. Preparação Matemática (One-Hot Encoding) na Camada Gold
python src/03_gold.py

# 5. Executar o inspetor de conformidade
python src/04_inspect_lgpd.py
```

---

**Desenvolvido por:**
[Pedro Arthur](https://parthur.dev) - Estudante de Engenharia de Dados e Segurança