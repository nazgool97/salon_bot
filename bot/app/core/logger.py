"""Logger facade."""

"""Minimal logger facade.

Original shared logger module removed; provide get_logger compatible helper.
"""


import logging

__all__ = ["get_logger"]


def get_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(name or __name__)
