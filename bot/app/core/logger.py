import logging

__all__ = ["get_logger"]

# Установим глобальный уровень логирования
logging.basicConfig(
    level=logging.DEBUG,  # <-- ключевая строка
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)


def get_logger(name: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name or __name__)
    logger.setLevel(logging.DEBUG)  # <-- дополнительно для конкретного логгера
    return logger
