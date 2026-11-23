"""Persistencia das metricas na base analitica (PostgreSQL).

Idempotencia
------------
A tabela guarda UM snapshot por (data_referencia, categoria), garantido pela
chave primaria. A gravacao de um dia acontece numa unica transacao:

1. UPSERT (INSERT ... ON CONFLICT DO UPDATE): categoria que ja existe naquele
   dia e atualizada; categoria nova e inserida. Nunca duplica linhas.
2. Remove do dia as categorias que nao vieram nesta execucao, para o snapshot
   refletir exatamente o ultimo processamento.

Se algo falhar no meio, o rollback desfaz tudo: o painel nunca ve um dia
gravado pela metade. Re-rodar a mesma run quantas vezes for preciso produz
sempre o mesmo resultado.
"""

import logging
from contextlib import closing
from datetime import date

from psycopg2.extras import execute_values

from shopbrasil import config

log = logging.getLogger(__name__)

TABELA = config.TABELA_METRICAS

SQL_CRIAR_TABELA = f"""
CREATE TABLE IF NOT EXISTS {TABELA} (
    data_referencia DATE           NOT NULL,
    categoria       TEXT           NOT NULL,
    preco_medio     NUMERIC(12, 2) NOT NULL,
    preco_minimo    NUMERIC(12, 2) NOT NULL,
    preco_maximo    NUMERIC(12, 2) NOT NULL,
    qtd_produtos    INTEGER        NOT NULL CHECK (qtd_produtos > 0),
    run_id          TEXT           NOT NULL,
    atualizado_em   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT pk_{TABELA} PRIMARY KEY (data_referencia, categoria)
);
COMMENT ON TABLE {TABELA} IS
    'Panorama diario de precos por categoria (1 linha por dia e categoria). '
    'Alimentado pelo DAG shopbrasil_precos_por_categoria.';
"""

SQL_UPSERT = f"""
INSERT INTO {TABELA} (
    data_referencia, categoria, preco_medio, preco_minimo, preco_maximo,
    qtd_produtos, run_id
)
VALUES %s
ON CONFLICT (data_referencia, categoria) DO UPDATE SET
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    qtd_produtos  = EXCLUDED.qtd_produtos,
    run_id        = EXCLUDED.run_id,
    atualizado_em = now()
"""

SQL_REMOVER_CATEGORIAS_AUSENTES = f"""
DELETE FROM {TABELA}
WHERE data_referencia = %s
  AND NOT (categoria = ANY(%s))
"""


def criar_tabela(hook) -> str:
    """Cria a tabela de metricas se ainda nao existir (seguro rodar sempre)."""
    hook.run(SQL_CRIAR_TABELA)
    log.info("Tabela %s pronta.", TABELA)
    return TABELA


def salvar_metricas_do_dia(hook, data_referencia: date, metricas: list[dict], run_id: str) -> dict:
    """Grava o snapshot do dia de forma idempotente, numa unica transacao."""
    if not metricas:
        raise ValueError("Nenhuma metrica para gravar; o snapshot do dia nao sera alterado.")

    linhas = [
        (
            data_referencia,
            m["categoria"],
            m["preco_medio"],
            m["preco_minimo"],
            m["preco_maximo"],
            m["qtd_produtos"],
            run_id,
        )
        for m in metricas
    ]
    categorias = [m["categoria"] for m in metricas]

    with closing(hook.get_conn()) as conn:
        with conn:  # commit se tudo der certo, rollback se qualquer passo falhar
            with conn.cursor() as cur:
                execute_values(cur, SQL_UPSERT, linhas)
                gravadas = len(linhas)
                cur.execute(SQL_REMOVER_CATEGORIAS_AUSENTES, (data_referencia, categorias))
                removidas = cur.rowcount

    resumo = {
        "data_referencia": data_referencia.isoformat(),
        "linhas_gravadas": gravadas,
        "categorias_removidas": removidas,
    }
    log.info("Gravado em %s: %s", TABELA, resumo)
    return resumo
