"""Cliente da API de catalogo (FakeStore API).

Faz UMA tentativa por chamada e deixa qualquer erro subir: quem decide se
retenta e o Airflow (retries + backoff exponencial na task).
"""

import logging

import requests

from shopbrasil import config

log = logging.getLogger(__name__)


class ErroApiCatalogo(Exception):
    """A API de catalogo nao devolveu dados utilizaveis."""


def buscar_produtos_catalogo(
    base_url: str = config.API_BASE_URL,
    timeout: tuple[int, int] = config.API_TIMEOUT,
) -> list[dict]:
    """Busca todos os produtos e devolve apenas os campos usados pelo pipeline.

    Retorna: [{"id": 1, "categoria": "electronics", "preco": 109.95}, ...]

    Levanta:
        requests.RequestException: timeout, falha de conexao ou HTTP 4xx/5xx.
        ErroApiCatalogo: resposta em formato inesperado ou sem produtos.
    """
    url = f"{base_url}/products"
    log.info("Buscando produtos em %s", url)

    resposta = requests.get(url, timeout=timeout)
    resposta.raise_for_status()  # HTTP 4xx/5xx vira excecao

    try:
        produtos = resposta.json()
    except ValueError as erro:
        raise ErroApiCatalogo(f"Resposta da API nao e um JSON valido: {erro}") from erro

    if not isinstance(produtos, list) or not produtos:
        # Uma lista vazia com HTTP 200 e tratada como instabilidade: gravar
        # "zero produtos" apagaria o panorama do dia no painel.
        raise ErroApiCatalogo(f"API devolveu uma lista vazia ou invalida: {str(produtos)[:200]}")

    try:
        return [
            {"id": p["id"], "categoria": p["category"], "preco": float(p["price"])}
            for p in produtos
        ]
    except (KeyError, TypeError, ValueError) as erro:
        raise ErroApiCatalogo(f"Produto fora do formato esperado: {erro!r}") from erro
