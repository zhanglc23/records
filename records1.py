import sys
import os


from docopt import docopt
from sqlalchemy import create_engine, exc, inspect, text


def cli():
    # 字符串split()函数 ，默认是按照空格区分
    supported_formats = 'csv tsv json yaml html xls xlsx dbf latex ods'.split()
    # 将 supported_formats数组按照 , 拼接
    formats_lst = ", ".join(supported_formats)

    # cli_docs 三引号注释
    cli_docs = """Records: SQL for Humans™
A Kenneth Reitz project.

Usage:
  records <query> [<format>] [<params>...] [--url=<url>]
  records (-h | --help)

Options:
  -h --help     Show this screen.
  --url=<url>   The database URL to use. Defaults to $DATABASE_URL.

Supported Formats:
   %(formats_lst)s

   Note: xls, xlsx, dbf, and ods formats are binary, and should only be
         used with redirected output e.g. '$ records sql xls > sql.xls'.

Query Parameters:
    Query parameters can be specified in key=value format, and injected
    into your query in :key format e.g.:

    $ records 'select * from repos where language ~= :lang' lang=python

Notes:
  - While you may specify a database connection string with --url, records
    will automatically default to the value of $DATABASE_URL, if available.
  - Query is intended to be the path of a SQL file, however a query string
    can be provided instead. Use this feature discernfully; it's dangerous.
  - Records is intended for report-style exports of database queries, and
    has not yet been optimized for extremely large data dumps.
    """ % dict(formats_lst=formats_lst)

    # 获取参数命令
    arguments = docopt(cli_docs)
    # 获取参数
    query = arguments['<query>']
    # 获取参数
    params = arguments['<params>']
    # 获取参数
    format = arguments.get('<format>')
    # 如果
    if format and "=" in format:
        del arguments['<format>']
        arguments['<params>'].append(format)
        format = None
    if format and format not in supported_formats:
        print('%s format not supported.' % format)
        print('Supported formats are %s.' % formats_lst)
        exit(62)

    # 解析没一个参数，如果格式不对则退出
    try:
        params = dict([i.split('=') for i in params])
    except ValueError:
        print('Parameters must be given in key=value format.')
        exit(64)
    
    # Be ready to fail on missing packages
    try:
        # Create the Database.
        db = Database(arguments['--url'])

        # Execute the query, if it is a found file.
        if os.path.isfile(query):
            rows = db.query_file(query, **params)

        # Execute the query, if it appears to be a query string.
        elif len(query.split()) > 2:
            rows = db.query(query, **params)

        # Otherwise, say the file wasn't found.
        else:
            print('The given query could not be found.')
            exit(66)

        # Print results in desired format.
        if format:
            content = rows.export(format)
            if isinstance(content, bytes):
                print_bytes(content)
            else:
                print(content)
        else:
            print(rows.dataset)
    except ImportError as impexc:
        print(impexc.msg)
        print("Used database or format require a package, which is missing.")
        print("Try to install missing packages.")
        exit(60)


    
# 数据库配置
class Database(object):
    """A Database. Encapsulates a url and an SQLAlchemy engine with a pool of
    connections.
    """
    # 初始化行数  **kwargs  标识可变参数
    def __init__(self, db_url=None, **kwargs):
        # 如果未设置数据库url，则获取 环境变量中 DATABASE_URL
        self.db_url = db_url or os.environ.get('DATABASE_URL')

        # 数据库URl 为空 则抛出异常
        if not self.db_url:
            raise ValueError('You must provide a db_url.')

        # 创建引擎 此方法待研究
        self._engine = create_engine(self.db_url, **kwargs)
        # 设置open状态
        self.open = True

    # 关闭连接
    def close(self):
        """Closes the Database."""
        # 关闭数据库
        self._engine.dispose()
        # 设置open状态
        self.open = False

    # entry 方法
    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Database open={}>'.format(self.open)

    def get_table_names(self, internal=False):
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return inspect(self._engine).get_table_names()

    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self.open:
            raise exc.ResourceClosedError('Database closed.')

        return Connection(self._engine.connect())

    def query(self, query, fetchall=False, **params):
        """Executes the given SQL query against the Database. Parameters can,
        optionally, be provided. Returns a RecordCollection, which can be
        iterated over to get result rows as dictionaries.
        """
        with self.get_connection() as conn:
            return conn.query(query, fetchall, **params)

    def bulk_query(self, query, *multiparams):
        """Bulk insert or update."""

        with self.get_connection() as conn:
            conn.bulk_query(query, *multiparams)

    def query_file(self, path, fetchall=False, **params):
        """Like Database.query, but takes a filename to load a query from."""

        with self.get_connection() as conn:
            return conn.query_file(path, fetchall, **params)

    def bulk_query_file(self, path, *multiparams):
        """Like Database.bulk_query, but takes a filename to load a query from."""

        with self.get_connection() as conn:
            conn.bulk_query_file(path, *multiparams)

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""

        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except:
            tx.rollback()
        finally:
            conn.close()



if __name__ == "__main__":
    cli()