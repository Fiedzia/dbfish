import os
from textwrap import dedent

import pymysql


def get_conn():
    mysql_params = dict(
        host=os.environ.get('MYSQL_HOST'),
        user=os.environ.get('MYSQL_USER'),
        passwd=os.environ.get('MYSQL_PASSWORD'),
        db=os.environ.get('MYSQL_DATABASE'),
        port=int(os.environ.get('MYSQL_PORT', '3306'))
    )
    return pymysql.connect(**mysql_params)


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
