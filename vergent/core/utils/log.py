import logging


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)-8s [%(name)s:%(funcName)s] : %(message)s',
    )
