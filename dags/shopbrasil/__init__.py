"""Modulos de apoio do pipeline de precos por categoria da ShopBrasil.

O arquivo do DAG cuida apenas da orquestracao (ordem e paralelismo das tasks);
a logica fica aqui, separada por responsabilidade:

- config     constantes do pipeline (API, retries, banco, pool, timezone)
- api        cliente da API de catalogo (uma tentativa; o Airflow retenta)
- callbacks  callbacks de ciclo de vida (retry, falha/alerta, sucesso)
- metricas   calculos puros sobre os produtos (sem Airflow, faceis de testar)
- db         DDL e gravacao idempotente (UPSERT) na base analitica
"""
