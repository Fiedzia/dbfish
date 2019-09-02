import os
from textwrap import dedent

from postgresql.driver import dbapi20


def get_conn():
    postgres_params = dict(
        host=os.environ.get('POSTGRES_HOST'),
        user=os.environ.get('POSTGRES_USER'),
        passwd=os.environ.get('POSTGRES_PASSWORD'),
        db=os.environ.get('POSTGRES_DATABASE'),
        port=int(os.environ.get('POSTGRES_PORT', '3306'))
    )
    return dbapi20.connect(**postgres_params)


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
