"""
### ShopBrasil - Panorama de precos por categoria

Substitui o script Python agendado via cron que alimentava o painel da equipe
de pricing. Roda todo dia as **06:00 (America/Sao_Paulo)**, coleta os produtos
da API de catalogo (FakeStore API), calcula preco medio, minimo, maximo e
quantidade de produtos por categoria e grava o resultado no PostgreSQL.

#### Topologias
| Topologia | Onde |
|---|---|
| **Linear** | `ingestao.buscar_produtos` -> `ingestao.agrupar_por_categoria` |
| **Fan-out** | `analise.calcular_metricas`: uma task por categoria (Dynamic Task Mapping) |
| **Fan-in** | `analise.consolidar_metricas`: reune as metricas de todas as categorias |

#### TaskGroups
- `ingestao`: coleta e prepara os dados da API
- `analise`: calculo paralelo das metricas e consolidacao
- `persistencia`: gravacao idempotente na base analitica
"""

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil import config, db
from shopbrasil.api import ErroApiCatalogo, buscar_produtos_catalogo
from shopbrasil.callbacks import alertar_falha, notificar_retry, registrar_sucesso
from shopbrasil.metricas import agrupar_precos_por_categoria, calcular_metricas_categoria

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Ingestao
# ---------------------------------------------------------------------------
@task(
    retries=config.API_RETRIES,
    retry_delay=config.API_RETRY_DELAY,
    retry_exponential_backoff=True,
    max_retry_delay=config.API_MAX_RETRY_DELAY,
    on_retry_callback=notificar_retry,
    on_failure_callback=alertar_falha,
    on_success_callback=registrar_sucesso,
)
def buscar_produtos() -> list[dict]:
    """Task critica: coleta os produtos da API de catalogo.

    Se a API oscilar, a excecao e relancada (raise) para o Airflow retentar
    com backoff exponencial. Esgotadas as tentativas, o callback de falha
    dispara o alerta.
    """
    # Lido na execucao (nao no topo do arquivo) para nao consultar o banco de
    # metadados a cada parse do DAG.
    base_url = Variable.get(config.VAR_API_BASE_URL, default_var=config.API_BASE_URL)

    try:
        produtos = buscar_produtos_catalogo(base_url=base_url)
    except requests.Timeout as erro:
        log.warning("Timeout ao consultar a API de catalogo: %s", erro)
        raise
    except requests.HTTPError as erro:
        log.warning("API de catalogo respondeu com erro HTTP: %s", erro)
        raise
    except requests.RequestException as erro:
        log.warning("Falha de conexao com a API de catalogo: %s", erro)
        raise
    except ErroApiCatalogo as erro:
        log.warning("Resposta inesperada da API de catalogo: %s", erro)
        raise

    log.info("%d produtos recebidos da API.", len(produtos))
    return produtos


@task
def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
    """Agrupa os precos por categoria. Cada item vira uma task no fan-out."""
    lotes = agrupar_precos_por_categoria(produtos)
    log.info(
        "%d categorias encontradas: %s",
        len(lotes),
        ", ".join(lote["categoria"] for lote in lotes),
    )
    return lotes


# ---------------------------------------------------------------------------
# Analise
# ---------------------------------------------------------------------------
@task(pool=config.POOL_ECOMMERCE, map_index_template="{{ categoria }}")
def calcular_metricas(lote: dict) -> dict:
    """Calcula as metricas de UMA categoria (executada em paralelo, limitada pelo pool)."""
    # Nomeia a instancia mapeada no Grid com a categoria em vez de 0, 1, 2...
    get_current_context()["categoria"] = lote["categoria"]

    metricas = calcular_metricas_categoria(lote["categoria"], lote["precos"])
    log.info("Metricas de '%s': %s", lote["categoria"], metricas)
    return metricas


@task
def consolidar_metricas(metricas_por_categoria: list[dict]) -> list[dict]:
    """Reune o resultado de todas as tasks mapeadas em uma unica lista."""
    consolidado = sorted(metricas_por_categoria, key=lambda m: m["categoria"])
    log.info("Metricas consolidadas de %d categorias.", len(consolidado))
    return consolidado


# ---------------------------------------------------------------------------
# Persistencia
# ---------------------------------------------------------------------------
@task
def criar_tabela() -> str:
    """Garante que a tabela de metricas exista (CREATE TABLE IF NOT EXISTS)."""
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    return db.criar_tabela(hook)


@task
def salvar_metricas(
    metricas: list[dict], tabela: str, data_interval_end=None, run_id=None
) -> dict:
    """Grava o snapshot do dia de forma idempotente (UPSERT numa transacao).

    `tabela` vem de criar_tabela: alem de informativo, e o que cria a
    dependencia criar_tabela -> salvar_metricas pela chamada das funcoes.
    """
    # A run das 06:00 do dia D cobre o intervalo (D-1 06:00, D 06:00]. Para o
    # painel, a data de referencia e o dia D, ou seja, o fim do intervalo.
    # Reprocessar a mesma run gera a mesma data, e o UPSERT sobrescreve.
    data_referencia = data_interval_end.in_timezone(config.TIMEZONE).date()
    log.info("Gravando %d categorias em %s (data_referencia=%s).", len(metricas), tabela, data_referencia)

    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    return db.salvar_metricas_do_dia(hook, data_referencia, list(metricas), run_id)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Panorama diario de precos por categoria para o time de pricing",
    schedule=config.SCHEDULE,
    start_date=pendulum.datetime(2026, 11, 1, tz=config.TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "time-dados",
        # Padrao para as demais tasks: uma retentativa e alerta se falhar.
        # A task critica (buscar_produtos) sobrescreve com a propria politica.
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": alertar_falha,
    },
    tags=["shopbrasil", "ecommerce", "precos"],
    doc_md=__doc__,
)
def shopbrasil_precos_por_categoria():
    @task_group(group_id="ingestao", tooltip="Coleta e preparo dos dados da API de catalogo")
    def ingestao():
        produtos = buscar_produtos()
        return agrupar_por_categoria(produtos)

    @task_group(group_id="analise", tooltip="Metricas por categoria em paralelo + consolidacao")
    def analise(lotes):
        metricas = calcular_metricas.expand(lote=lotes)  # fan-out
        return consolidar_metricas(metricas)  # fan-in

    @task_group(group_id="persistencia", tooltip="Gravacao idempotente no PostgreSQL")
    def persistencia(metricas):
        tabela = criar_tabela()
        salvar_metricas(metricas=metricas, tabela=tabela)

    persistencia(analise(ingestao()))


shopbrasil_precos_por_categoria()
