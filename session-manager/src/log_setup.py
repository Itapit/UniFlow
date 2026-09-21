"""Shared stdout structured logging for session-manager.

Usage: logger = get_logger("aggregator") then
logger.info("event=block_ready file_hash=%s block_id=%d", file_hash, block_id)
Level comes from UNIFLOW_LOG_LEVEL (DEBUG/INFO/WARN/ERROR), default INFO.
"""
import logging
import os
import sys

_configured = False


def _level_from_env():
    return getattr(logging, os.environ.get("UNIFLOW_LOG_LEVEL", "INFO").upper(), logging.INFO)


def setup_root_logger():
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s level=%(levelname)s component=%(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root = logging.getLogger("uniflow")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(_level_from_env())
    root.propagate = False
    _configured = True


def get_logger(component: str) -> logging.Logger:
    setup_root_logger()
    return logging.getLogger(f"uniflow.{component}")
