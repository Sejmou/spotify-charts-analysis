"""
A hacky script to execute commands stored in .sql files on ClickHouse DB, optionally replacing strings in the SQL file before execution.

key-value pair parsing for string replacement functionality stolen from https://stackoverflow.com/a/52014520/13727176
"""
import argparse
import clickhouse_connect


def main(sql_file: str, strs_to_replace: dict = None):
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="spotify_charts",
    )
    try:
        with open(sql_file, "r") as f:
            sql = f.read()
            if strs_to_replace is not None:
                for k, v in strs_to_replace.items():
                    sql = sql.replace(k, v)
            result = client.command(sql)
            print(result)
    except Exception as e:
        print(e)
    finally:
        client.close()


def parse_var(s):
    """
    Parse a key, value pair, separated by '='
    That's the reverse of ShellArgs.

    On the command line (argparse) a declaration will typically look like:
        foo=hello
    or
        foo="hello world"
    """
    items = s.split("=")
    key = items[0].strip()  # we remove blanks around keys, as is logical
    if len(items) > 1:
        # rejoin the rest:
        value = "=".join(items[1:])
    return (key, value)


def parse_vars(items):
    """
    Parse a series of key-value pairs and return a dictionary
    """
    d = {}

    if items:
        for item in items:
            key, value = parse_var(item)
            d[key] = value
    return d


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "sql_file",
        type=str,
        help="Path to SQL file with command that should be executed on ClickHouse DB",
    )
    parser.add_argument(
        "--replace",
        metavar="KEY=VALUE",
        nargs="*",  # 0 or more
        help="A collection of key-value pairs of strings that should be replaced in the SQL file content before passing it to ClickHouse. "
        "(do not put spaces before or after the = sign). "
        "If a value contains spaces, you should define "
        "it with double quotes: "
        'foo="this is a sentence". Note that '
        "values are always treated as strings.",
    )

    args = parser.parse_args()
    sql_file = args.sql_file
    values_to_replace = parse_vars(args.replace)

    main(sql_file, values_to_replace)
