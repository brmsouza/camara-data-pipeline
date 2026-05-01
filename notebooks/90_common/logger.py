# Databricks notebook source

from __future__ import annotations

import logging


def get_logger(name: str, layer: str = "pipeline") -> logging.Logger:
    """
    Cria um logger padronizado para os notebooks do pipeline.
    """
    layer_norm = layer.strip().lower()
    logger_name = f"{layer_norm}.{name}"

    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(layer)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    class LayerFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.layer = layer_norm.upper()
            return True

    handler.addFilter(LayerFilter())
    logger.addHandler(handler)

    return logger