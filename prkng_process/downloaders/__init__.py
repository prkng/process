import os

from ..database import PostgresWrapper


class DataSource(object):
    """
    Base class for datasource
    """
    def __init__(self):
        self.db = PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))


def script(src):
    """returns the location of sql scripts"""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', src)
