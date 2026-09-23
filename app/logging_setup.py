"""日志配置。

单测中通过 pytest 的 log_cli 打印判定依据（见 pyproject.toml）。
"""

import logging

_CONFIGURED = False

_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """配置根 logger（幂等，可重复调用）。"""

    global _CONFIGURED
    if not _CONFIGURED:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_FORMAT))
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(level)
        _CONFIGURED = True
    return logging.getLogger("guardrails")


def get_logger(name: str) -> logging.Logger:
    """获取带统一前缀的 logger。"""

    return logging.getLogger(f"guardrails.{name}")
