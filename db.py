from sqlalchemy import create_engine
from google.cloud import bigquery
import sqlglot
from enum import Enum
from sqlglot.dialects import TSQL
from sqlglot import exp


class DBMS(Enum):
    BIGQUERY = 1
    SQL_SERVER = 2
    SQLITE = 3


DBMS_NAMES = {
    'bigquery': DBMS.BIGQUERY,
    'sql_server': DBMS.SQL_SERVER,
    'sqlite': DBMS.SQLITE
}


class DBAdapter():
    pass