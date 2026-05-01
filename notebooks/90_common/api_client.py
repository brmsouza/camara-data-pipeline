# Databricks notebook source

from __future__ import annotations

from typing import Any

import requests


BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
DEFAULT_TIMEOUT = 30


def get_data(
    endpoint: str,
    params: dict[str, Any] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    Executa uma chamada GET na API da Câmara dos Deputados.
    """
    url = f"{BASE_URL}{endpoint}"

    logger.info(
        "request_started | endpoint=%s | params=%s",
        endpoint,
        params,
    )

    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()

        payload = response.json()
        records_count = len(payload.get("dados", []))

        logger.info(
            "request_completed | endpoint=%s | status_code=%s | records=%s",
            endpoint,
            response.status_code,
            records_count,
        )

        return payload

    except requests.exceptions.RequestException:
        logger.exception(
            "request_failed | endpoint=%s | params=%s",
            endpoint,
            params,
        )
        raise