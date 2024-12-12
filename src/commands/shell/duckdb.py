import os
from textwrap import dedent

import duckdb


def get_conn():
    return duckdb.connect(os.environ.get('SQLITE_FILE', ':memory:'))


def main():
    msg = dedent("""
    Variables and functions:
        conn: database connection
        cursor: connection cursor
        get_conn(): obtain database connection
        msg: function printing this message
    """)
    print(msg)
    import IPython
    conn = get_conn()
    IPython.start_ipython(argv=[], user_ns={
        'conn': conn,
        'cursor': conn.cursor(),
        'get_conn': get_conn,
        'msg': lambda: print(msg)
    })
    raise SystemExit


if __name__ == '__main__':
    main()
