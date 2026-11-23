"""Calculos sobre os produtos do catalogo.

Funcoes puras: recebem dados e devolvem dados, sem depender do Airflow.
Assim podem ser testadas isoladamente e reaproveitadas por outros pipelines.
"""

from collections import defaultdict
from statistics import mean


def agrupar_precos_por_categoria(produtos: list[dict]) -> list[dict]:
    """Agrupa os precos dos produtos pela categoria.

    As categorias saem dos proprios dados: se a API passar a devolver uma
    categoria nova, ela aparece aqui sem nenhuma mudanca de codigo.

    Retorna uma lista (ordenada por categoria) no formato
    [{"categoria": "electronics", "precos": [109.0, 64.0, ...]}, ...],
    pronta para ser usada no .expand() do Dynamic Task Mapping.
    """
    precos_por_categoria: dict[str, list[float]] = defaultdict(list)
    for produto in produtos:
        precos_por_categoria[produto["categoria"]].append(float(produto["preco"]))

    return [
        {"categoria": categoria, "precos": precos}
        for categoria, precos in sorted(precos_por_categoria.items())
    ]


def calcular_metricas_categoria(categoria: str, precos: list[float]) -> dict:
    """Calcula o panorama de precos de uma categoria."""
    if not precos:
        raise ValueError(f"Categoria '{categoria}' sem precos para calcular metricas.")

    return {
        "categoria": categoria,
        "preco_medio": round(mean(precos), 2),
        "preco_minimo": round(min(precos), 2),
        "preco_maximo": round(max(precos), 2),
        "qtd_produtos": len(precos),
    }
