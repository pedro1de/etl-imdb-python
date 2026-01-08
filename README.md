# Pipeline ETL – Dados IMDb para Análise de Tendências de Mercado

## Contexto de Negócio

Este projeto simula um cenário real de uma empresa de mídia que está estruturando um novo squad de produto com foco em campanhas de marketing baseadas em lançamentos de filmes.

Para apoiar decisões estratégicas, surgiu a necessidade de compreender padrões de consumo, aceitação do público e tendências de mercado utilizando dados do IMDb (Internet Movie Database), uma das maiores bases públicas da indústria audiovisual.

Antes de qualquer análise, foi necessário construir um pipeline de ETL capaz de coletar, tratar e organizar esses dados de forma estruturada e confiável.

Este projeto foca exatamente nessa etapa.

---

## Objetivo do Projeto

Construir um pipeline de ETL automatizado para:

- Extrair dados públicos do IMDb
- Tratar e padronizar dados brutos
- Armazenar os dados em um banco relacional
- Criar tabelas analíticas prontas para consumo por times de dados, produto ou marketing

O projeto simula a atuação de um profissional responsável pela preparação da base de dados para análises e tomada de decisão.

---

## Tecnologias Utilizadas

- Python
- Pandas
- SQLite
- Requests
- Logging
- Schedule
- Git / GitHub

---

## Pipeline ETL

### Extração
- Download automatizado dos datasets públicos do IMDb
- Controle de existência para evitar downloads redundantes

### Transformação
- Leitura de arquivos compactados (.tsv.gz)
- Tratamento de valores nulos
- Padronização dos dados
- Geração de arquivos tratados no formato .tsv

### Carga
- Carga dos dados tratados em banco SQLite
- Escrita em chunks para lidar com grandes volumes de dados
- Criação de tabelas analíticas

---

## Modelo Analítico

Foram criadas tabelas analíticas contendo informações como:

- Títulos e tipos de produção
- Avaliações médias e volume de votos
- Gêneros
- Ano de lançamento
- Quantidade de participantes por produção

Essas tabelas servem como base para análises de tendências e apoio à tomada de decisão em campanhas de marketing.

---

## Observação

Devido a limitações momentâneas de hardware, foi utilizada uma versão reduzida do pipeline para testes e validação local.

O código completo do ETL, preparado para lidar com grandes volumes de dados, está disponível neste repositório e segue a mesma lógica de funcionamento.

---

## Possíveis Desdobramentos

- Desenvolvimento de dashboards analíticos
- Análises de tendências por gênero e período
- Apoio à definição de campanhas baseadas em lançamentos
- Exploração de padrões de aceitação do público

---

## Fonte dos Dados

IMDb Datasets  
https://datasets.imdbws.com/

---

## Autor

Pedro Oliveira  
Estudante de Ciência de Dados  
Projetos voltados para Engenharia e Análise de Dados
