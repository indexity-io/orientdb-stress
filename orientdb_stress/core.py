import logging


class OdbException(Exception):
    pass


LOG_FORMAT = logging.Formatter("%(asctime)s %(relativeCreated)6d [%(threadName)-30s] %(levelname).4s [%(name)20s] %(message)s")
