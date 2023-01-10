# Python Imports
from enum import Enum

# Third-Party Imports

from sqlalchemy import create_engine
from google.cloud import bigquery
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
    """Custom Dialect for for tSQL
    """

    class Parser(TSQL.Parser):
        # Ensures that tSQL's COUNT_BIG function is interpreted as the count operation
        FUNCTIONS = {
            **TSQL.Parser.FUNCTIONS, 'COUNT_BIG': exp.Count.from_arg_list
        }

    class Generator(TSQL.Generator):
        # Ensures that tSQL's COUNT_BIG function is interpreted as the count operation
        TRANSFORMS = {
            **TSQL.Generator.TRANSFORMS, exp.Count: rename_func('COUNT_BIG')
        }


# Add the custom dialect to the list of dialect classes
Dialect.classes['tsql_extension'] = TSQLExtension


# Enumerate the DBMS systems used
class DBMS(Enum):
    BIGQUERY = 1
    SQL_SERVER = 2
    SQLITE = 3


# Store dictionary of DBMS names as expected by sqlglot
DBMS_NAMES = {
    'bigquery': DBMS.BIGQUERY,
    'tsql': DBMS.SQL_SERVER,
    'sqlite': DBMS.SQLITE
}
