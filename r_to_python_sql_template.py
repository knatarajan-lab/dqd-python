import re
from pathlib import Path

R_SQL_DIR = "./DataQualityDashboard/inst/sql/sql_server"


def repl_fn(full_clause):
    condition_clause = full_clause.group(1)
    if_clause = full_clause.group(2)
    else_clause = full_clause.group(3) if len(
        full_clause.groups()) == 3 else None

    if else_clause:
        jinja_clause = \
            f"""{{% if {condition_clause} %}}{if_clause}{{% else %}}{else_clause}{{% endif %}}   
            """
    else:
        jinja_clause = \
            f"""{{% if {condition_clause} %}}{if_clause}{{% endif %}}   
            """

    return jinja_clause


def main():
    for fname in Path(R_SQL_DIR).glob("*.sql"):
        with open(fname, 'r') as f:
            script = f.read()

            conditional_clause_ptn = "\{([^}]+)\}\s*\?\s*\{([^}]+)\}(?:\s*:\s*{([^}]+)\})?"
            template_var_ptn = "@(\w+)"

            script = re.sub(conditional_clause_ptn,
                            repl_fn,
                            script,
                            flags=re.S)
            script = re.sub(template_var_ptn,
                            lambda s: f"{{{{{s.group(1)}}}}}", script)

        with open(Path('sql') / Path(fname).name, 'w') as f:
            f.write(script)


if __name__ == "__main__":
    main()