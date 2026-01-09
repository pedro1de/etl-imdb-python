# Pipeline ETL – Dados IMDb para Análise de Tendências de Mercado

## Contexto

Este projeto simula um cenário real de uma empresa de mídia que necessita estruturar sua base de dados para apoiar decisões estratégicas de marketing e produto, com foco em lançamentos audiovisuais.

Para isso, utiliza dados públicos do IMDb (Internet Movie Database), uma das maiores bases de dados da indústria audiovisual, exigindo a construção de um pipeline de ETL robusto, reprodutível e escalável antes de qualquer etapa analítica.

O escopo do projeto está concentrado na engenharia e preparação dos dados, não na análise final.

---

## Objetivo

Construir um pipeline de ETL em Python capaz de:

- Extrair dados públicos do IMDb  
- Tratar e padronizar dados brutos  
- Persistir os dados em um banco relacional  
- Criar tabelas analíticas prontas para consumo por áreas de negócio  

O projeto representa a atuação de um profissional responsável por estruturar a base de dados para análises posteriores.

---

## Tecnologias Utilizadas

- Python  
- Pandas  
- SQLite  
- Requests  
- Logging  
- Git / GitHub  

---

## Estrutura do Projeto

etl-imdb-python/
│
├── src/
│ ├── extract/
│ ├── transform/
│ ├── load/
│ ├── etl_imdb.py
│ ├── etl_imdb_full.py
│
├── notebooks/
│
├── data/
│
├── README.md
└── requirements.txt


- `etl_imdb.py`: versão reduzida do pipeline, utilizada para execução local e validação.
- `etl_imdb_full.py`: versão completa do pipeline, preparada para processar o conjunto integral de dados do IMDb.

---

## Pipeline ETL

### Extração
- Download automatizado dos datasets públicos do IMDb
- Verificação de existência local para evitar downloads redundantes

### Transformação
- Leitura de arquivos compactados (`.tsv.gz`)
- Tratamento de valores nulos
- Padronização dos dados
- Geração de arquivos tratados em formato `.tsv`

### Carga
- Carga dos dados tratados em banco SQLite
- Escrita em chunks para lidar com grandes volumes
- Criação de tabelas analíticas a partir dos dados normalizados

---

## Modelo Analítico

O pipeline gera tabelas analíticas contendo informações como:

- Identificação e tipo de produção  
- Avaliação média e volume de votos  
- Gêneros  
- Ano de lançamento  
- Quantidade de participantes por título  

Essas tabelas servem como base para análises de tendências e suporte à tomada de decisão em campanhas de marketing e produto.

---

## Observação sobre Escalabilidade

Este repositório contém duas versões do pipeline:

- **Versão reduzida (`etl_imdb.py`)**  
  Utilizada para testes locais e demonstração do funcionamento do ETL, limitando o volume de dados processados para evitar restrições de hardware.

- **Versão completa (`etl_imdb_full.py`)**  
  Preparada para processar o conjunto integral de dados do IMDb, utilizando escrita em disco, processamento em chunks e controle de memória. Indicada para execução em ambientes com maior capacidade computacional.

Ambas seguem a mesma lógica de negócio e arquitetura de pipeline.

---

## Possíveis Extensões

- Criação de dashboards analíticos  
- Análises de tendências por gênero, período ou avaliação  
- Integração com ferramentas de BI  
- Orquestração do pipeline em ambientes produtivos  

---

## Fonte dos Dados

IMDb Datasets  
https://datasets.imdbws.com/

---

## Autor

Pedro Oliveira  
Estudante de Ciência de Dados  
Projetos voltados para Engenharia e Análise de Dados
