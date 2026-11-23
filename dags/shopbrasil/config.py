"""Constantes do pipeline. Um unico lugar para mudar nomes e enderecos."""

from datetime import timedelta

import pendulum

# Agendamento
TIMEZONE = pendulum.timezone("America/Sao_Paulo")
SCHEDULE = "0 6 * * *"  # todo dia as 06:00 no horario de Brasilia

# Fonte de dados (API de catalogo)
API_BASE_URL = "https://fakestoreapi.com"
# Airflow Variable opcional que sobrescreve a URL sem novo deploy (ex.: apontar
# para homologacao, ou simular a API fora do ar nos testes de falha).
VAR_API_BASE_URL = "shopbrasil_api_base_url"
API_TIMEOUT = (5, 30)  # segundos: (conectar, ler a resposta)

# Retentativas da ingestao. Quem retenta e o Airflow (nao o requests): cada
# tentativa fica registrada na UI, com log proprio e callback de retry.
# Com backoff exponencial a espera dobra a cada falha (~20s, ~40s, ~80s...),
# dando tempo para a API se recuperar sem martela-la.
API_RETRIES = 3
API_RETRY_DELAY = timedelta(seconds=20)
API_MAX_RETRY_DELAY = timedelta(minutes=5)

# Recursos do Airflow (criados pelo airflow-init no docker-compose)
POSTGRES_CONN_ID = "postgres_analytics"
POOL_ECOMMERCE = "ecommerce_pool"

# Base analitica
TABELA_METRICAS = "metricas_preco_categoria"
