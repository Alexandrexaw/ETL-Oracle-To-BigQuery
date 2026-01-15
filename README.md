![Language](https://img.shields.io/badge/Language-English-blue)

# ETL-Oracle-To-BigQuery
Data pipeline developed in Python and orchestrated by Apache Airflow, designed to extract data from legacy Oracle systems (ERP) and dynamically load it into BigQuery.

## Key Features
**Chunked Extraction:** The script processes large volumes of data in blocks (block_size parameter), preventing Out-of-Memory (OOM) failures in the Airflow worker.
> This strategy was adopted due to the increasing volume of data in the source system.

**Dynamic Task Generation:** The pipeline reads metadata from the source database to dynamically create load tasks for each table. This allows any new tables created with the same prefix to be automatically loaded into BigQuery without code changes.

**Secret Vault Integration:** A security layer that consumes credentials via a Token-based API to retrieve access credentials securely.

**Date Handling:** Customized logic to handle years beyond 2199, ensuring compatibility with BigQuery and Pandas timestamp formats.

**Monitoring and Logs:** Utilizes google-cloud-logging for application logs, enhancing visibility across all processing stages.

## Technology Stack
- Orchestration: Apache Airflow (Google Cloud Composer).

- Language: Python 3.x.

- Data Warehouse: Google BigQuery.

- Source Database: Oracle Database (via python-oracledb).

- Data Processing: Pandas.

***
![Português](https://img.shields.io/badge/Idioma-Portugu%C3%AAs-green)

# ETL-Oracle-To-BigQuery

Pipeline de dados, desenvolvido em Python e orquestrado pelo Apache Airflow, projetado para extrair dados de sistemas legados Oracle (ERP) e carregá-los de forma dinamicamente no BigQuery.

## Principais Funcionalidades

**Extração por Chunking:** O script processa grandes volumes de dados em blocos (parâmetro block_size), evitando falhas por excesso de memória (OOM) no worker do Airflow.
> Estratégia adotada devido ao aumento no volume de dados no sistema fonte.

**Geração Dinâmica de Tasks:** O pipeline lê os metadados do banco de origem para criar tasks de carga dinamicamente para cada tabela, permitindo que novas tabelas criadas contendo o mesmo prefixo sejam carregadas no BigQuery.

**Integração com Cofre de Senhas:** Camada de segurança que consome credenciais via API (Token-based) para consultar as credencias de acesso.

**Tratamento de Datas:** Lógica customizada para tratar anos superiores a 2199 para  compatibilidade com o formato de timestamp do BigQuery e Pandas.

**Monitoramento e Logs:** Utilizado google-cloud-logging para log da aplicação, facilitando a visibilidade das etapas de processamento.

## Tecnologias envolvidas

- Orquestração: Apache Airflow (Google Cloud Composer).

- Linguagem: Python 3.x.

- Data Warehouse: Google BigQuery.

- Banco de Origem: Oracle Database (via python-oracledb).

- Processamento de Dados: Pandas.

