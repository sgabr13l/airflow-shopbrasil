"""Callbacks de ciclo de vida das tasks.

- on_retry_callback   uma tentativa falhou, mas ainda vai retentar (aviso)
- on_failure_callback falhou de vez, sem retentativas restantes (alerta)
- on_success_callback terminou com sucesso (registro para auditoria)

O alerta e simulado com um log em nivel ERROR num formato facil de achar.
Em producao, o mesmo ponto enviaria a mensagem para Slack, e-mail, Teams etc.
"""

import logging

log = logging.getLogger(__name__)

CANAL_ALERTAS = "#dados-alertas"  # canal ficticio do time de dados


def _identificar(context) -> tuple:
    ti = context["task_instance"]
    return ti, context["dag_run"].run_id


def notificar_retry(context) -> None:
    ti, run_id = _identificar(context)
    proxima = ti.next_retry_datetime()
    log.warning(
        "[callback] RETRY | task=%s run=%s | tentativa %s de %s falhou: %s | "
        "proxima tentativa as %s",
        ti.task_id,
        run_id,
        ti.try_number,
        ti.max_tries + 1,
        context.get("exception"),
        proxima.isoformat() if proxima else "?",
    )


def alertar_falha(context) -> None:
    ti, run_id = _identificar(context)
    mensagem = (
        f"Pipeline de precos FALHOU | dag={ti.dag_id} | task={ti.task_id} | "
        f"run={run_id} | tentativas={ti.try_number} | erro={context.get('exception')} | "
        f"log={ti.log_url}"
    )
    log.error("[callback] ALERTA enviado para %s (simulado): %s", CANAL_ALERTAS, mensagem)


def registrar_sucesso(context) -> None:
    ti, run_id = _identificar(context)
    log.info(
        "[callback] SUCESSO | task=%s run=%s | concluida na tentativa %s",
        ti.task_id,
        run_id,
        ti.try_number,
    )
