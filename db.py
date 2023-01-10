from sqlalchemy import create_engine
from google.cloud import bigquery

from enum import Enum
from sqlglot.dialects import TSQL, Dialect
from sqlglot import exp
from sqlglot.dialects.dialect import rename_func

# def _date_add_sql():
#     def func(self, expression):
#         this = self.sql(expression, "this")
#         unit = self.sql(expression, "unit") or "'day'"
#         expression = self.sql(expression, "expression")
#         return f"{data_type}_{kind}({this}, INTERVAL {expression} {unit})"


#     return func
class TSQLExtension(TSQL):

    class Parser(TSQL.Parser):
        FUNCTIONS = {
            **TSQL.Parser.FUNCTIONS, 'COUNT_BIG': exp.Count.from_arg_list
        }

    class Generator(TSQL.Generator):
        TRANSFORMS = {
            **TSQL.Generator.TRANSFORMS, exp.Count: rename_func('COUNT_BIG')
        }


Dialect.classes['tsql_extension'] = TSQLExtension


class DBMS(Enum):
    BIGQUERY = 1
    SQL_SERVER = 2
    SQLITE = 3


DBMS_NAMES = {
    'bigquery': DBMS.BIGQUERY,
    'tsql': DBMS.SQL_SERVER,
    'sqlite': DBMS.SQLITE
}


class DBAdapter():
    pass