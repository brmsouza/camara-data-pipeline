# Databricks notebook source

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, urlparse


def get_next_link(payload: dict[str, Any]) -> str | None:
    """Retorna o link da próxima página, quando existir."""
    for link in payload.get("links", []):
        if link.get("rel") == "next":
            return link.get("href")

    return None


def extract_endpoint_and_params(url: str) -> tuple[str, dict[str, Any]]:
    """Converte uma URL completa da API em endpoint e parâmetros."""
    parsed = urlparse(url)

    endpoint = parsed.path.replace("/api/v2", "")
    query_params = parse_qs(parsed.query)

    params = {
        key: values[0] if len(values) == 1 else values
        for key, values in query_params.items()
    }

    return endpoint, params


def paginate(
    endpoint: str,
    params: dict[str, Any] | None = None,
    max_pages: int | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Itera sobre as páginas disponíveis da API.
    Mantém baixo uso de memória ao retornar uma página por vez.
    """
    current_endpoint = endpoint
    current_params = params or {}
    page = 1

    logger.info(
        "pagination_started | endpoint=%s | params=%s | max_pages=%s",
        endpoint,
        current_params,
        max_pages,
    )

    while True:
        payload = get_data(current_endpoint, current_params)
        records = len(payload.get("dados", []))

        logger.info(
            "page_loaded | endpoint=%s | page=%s | records=%s",
            endpoint,
            page,
            records,
        )

        yield payload

        if max_pages is not None and page >= max_pages:
            logger.info(
                "pagination_stopped_by_limit | endpoint=%s | pages=%s",
                endpoint,
                page,
            )
            break

        next_link = get_next_link(payload)

        if not next_link:
            logger.info(
                "pagination_completed | endpoint=%s | total_pages=%s",
                endpoint,
                page,
            )
            break

        current_endpoint, current_params = extract_endpoint_and_params(next_link)
        page += 1