# 🛡️ Secure Lakehouse Pipeline: Engenharia de Dados & Adequação LGPD

Este projeto simula a construção de um Data Lakehouse corporativo focado em segurança da informação, privacidade de dados (LGPD) e preparação de _features_ para modelos de Machine Learning (Previsão de Churn).

O objetivo principal não é treinar a IA, mas arquitetar a infraestrutura de dados que permite aos cientistas de dados trabalharem com total conformidade legal, garantindo que Informações Pessoais Identificáveis (PII) sejam criptografadas e isoladas da camada de _Analytics_.

## 🏗️ Arquitetura e Fluxo de Dados

O pipeline foi construído sobre a **Arquitetura Medallion**, utilizando **Apache Spark (PySpark)** para processamento distribuído e **Delta Lake** para garantir transações ACID, _Time Travel_ e confiabilidade na escrita.

1. **Simulação de Caos (Silos Corporativos):** Um _dataset_ de Churn foi enriquecido com PIIs sintéticas (CPFs, Nomes, Emails) via biblioteca `Faker` e fragmentado em dois sistemas inconsistentes: um CRM (Atendimento) e um ERP (Faturamento com dados nulos).
2. **Camada Bronze (Ingestão Raw):** Ingestão bruta dos silos em formato Delta. Preservação do histórico imutável sem transformações.
3. **Camada Silver (Núcleo de Segurança):** Integração dos sistemas (JOIN) e sanitização. Aplicação da governança cibernética para destruir a PII direta.
4. **Camada Gold (Machine Learning Features):** Engenharia de _features_ binarizando variáveis categóricas (One-Hot Encoding) e tipagem estrita para consumo direto por algoritmos de IA.
5. **Orquestração e Observabilidade:** Automação do fluxo via **Prefect** com DAG (Directed Acyclic Graph), isolando subprocessos e garantindo tolerância a falhas e monitoramento em tempo real.

## 🔐 Engenharia de Segurança Cibernética

Para garantir a total conformidade com a LGPD e mitigar riscos de vazamento (_Data Breach_), a arquitetura implementa:

- **Salted Hashing (SHA-256):** CPFs não são apenas codificados. Um _Secret Salt_ é concatenado ao dado antes do _hash_, tornando ataques de engenharia reversa (_Rainbow Tables_) matematicamente inviáveis.
- **Data Masking (Mascaramento Dinâmico):** E-mails e telefones são ofuscados usando Expressões Regulares (Regex) nativas do PySpark (`p***@gmail.com`).
- **Gestão de Segredos:** Remoção total de chaves expostas no código (_Hardcoded Secrets_) através da injeção dinâmica via variáveis de ambiente (`.env`).
- **Testes Automatizados (Pytest):** Cobertura de testes unitários no módulo de criptografia para garantir que a lógica de segurança não seja corrompida em atualizações futuras.

### 📋 Evidência de Auditoria (PoC)

Abaixo está o registro real gerado pelo motor de auditoria (`src/04_inspect_lgpd.py`), comprovando a transformação criptográfica entre as camadas:

<img width="739" height="579" alt="Image" src="https://github.com/user-attachments/assets/70bba4c3-93e7-4cf8-ad5d-c3108701ddea" />

## 🚀 Como Executar Localmente

### 1. Instalação e Configuração

Recomenda-se o uso do Python 3.11 para máxima estabilidade.

```bash
python -m venv venv

# Linux / macOS:
source venv/bin/activate
# Windows
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 2. Configuração de Segurança

Crie um arquivo `.env` na raiz do projeto e defina uma chave secreta forte para o Salt cripográfico:

```
LGPD_SALT_KEY=SuaChaveSecretaAqui123!
```

### 3. Executando os Testes Unitários

Valide a integridade dos algoritmos de segurança antes de rodar o pipeline:

``` bash
pytest tests/
```

### 4. Execução da Esteira Orquestrada (Prefect)

O pipeline inteiro é gerenciado pelo orquestrador. Abra dois terminais com o `venv` ativado:

#### Terminal 1 - Painel de Observabilidade:

``` bash
prefect server start
```

*Acesse a interface web gerada para monitorar a execução em tempo real.*

#### Terminal 2 - Disparo do Pipeline:

``` bash
python src/orchestrator.py
```

---

**Desenvolvido por:**
[Pedro Arthur](https://parthur.dev) - Estudante de Engenharia e Segurança de Dados.
