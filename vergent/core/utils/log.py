import logging


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(name)s:%(funcName)s] %(levelname)-8s : %(message)s',
    )
