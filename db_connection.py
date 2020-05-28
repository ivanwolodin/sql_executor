import random
import sqlite3
import time

import pymysql

from logger import logger


class DataBase:
    """ creates db and fills it with values"""

    def __init__(self, which_database=':memory:'):
        self._conn = None
        self._cursor = None

        self._available_database_type = ['MySQL', ]

        self._available_actions = ['SELECT', 'UPDATE', 'INSERT', 'DELETE', ]
        self._currently_not_available = ['CREATE', 'DROP', ]
        self.which_database = ''

        self.capture_database(database_type=which_database)
        self.which_database = which_database

    @staticmethod
    def str_time_prop(start, end, format, prop):
        stime = time.mktime(time.strptime(start, format))
        etime = time.mktime(time.strptime(end, format))

        ptime = stime + prop * (etime - stime)
        return time.strftime(format, time.localtime(ptime))

    @staticmethod
    def random_date(start, end, prop):
        return DataBase.str_time_prop(start, end, '%Y.%m.%d', prop)

    def create_db_in_memory(self) -> None:
        """
        create simple table stocks

        production_date | status | salesman | real_value | price

        :return:
        """

        self._cursor.execute('CREATE TABLE stocks '
                             '(production_date text,'
                             ' status text,'
                             ' salesman text,'
                             ' real_value int,'
                             ' price real)')

        for i in range(10):
            self._cursor.execute("INSERT INTO stocks VALUES "
                                 "('{}', "
                                 " '{}', "
                                 " '{}', "
                                 "  {},  "
                                 "  {} "
                                 ") ".format(
                                     DataBase.random_date('2008.01.01',
                                                          '2020.01.01',
                                                          random.random()),
                                     random.choice(['Buy', 'Sell', 'Rent']),
                                     random.choice(
                                         ['Lisa',
                                          'John',
                                          'Hanna',
                                          'Peter',
                                          'Alice']),
                                     random.randint(10, 100),
                                     round(random.uniform(30, 200), 2))
                                 )

        self._conn.commit()

    def execute_sql(self, sql_statement):

        status, action = self._validate_sql(sql_statement=sql_statement)
        if not status:
            return action

        try:
            if action in ['UPDATE', 'INSERT', 'DELETE']:
                self._cursor.execute(sql_statement)
                self._conn.commit()
                if self._cursor.rowcount >= 1:
                    return [['{} rows affected'.format(self._cursor.rowcount)]]
                else:
                    return [['Zero rows affected!']]
            else:
                self._cursor.execute(sql_statement)
                res = self._cursor.fetchall()
                if not res:
                    return [['empty response, no such data']]
                return res

        except Exception as e:
            print(e)
            logger.info(
                'Wrong sql syntax or missing table.'
                ' sql_statement={}.'
                ' Error: {}'.format(sql_statement, e))
            return [['Check sql syntax or no such table']]

    def _validate_sql(self, sql_statement):
        try:
            action = sql_statement.split(' ', 1)[0]
        except Exception as e:
            logger.info(
                'Not sql request. sql_statement={}. Error={}'.format(
                    sql_statement, e))
            return False, [['Cannot validate sql']]

        action = action.upper()
        if action not in self._available_actions:
            if action in self._currently_not_available:
                return False, [
                    ['Action {} is not implemented yet'.format(action)]]

            if action not in self._currently_not_available:
                return False, [['Check sql syntax']]

        return True, action

    def capture_database(self, database_type=':memory:', **kwargs) -> bool:
        if database_type == self.which_database:
            # print('Db has not changed')
            return True

        if database_type == ':memory:':
            try:
                self._conn = sqlite3.connect(
                    'file::memory:?cache=shared', uri=True)
                self._cursor = self._conn.cursor()

            except Exception as e:
                self._conn.close()
                logger.error('Cannot connect to :memory: Error: {}'.format(e))
                print(e)
                return False
            else:
                self.which_database = ':memory:'
                self.create_db_in_memory()
                return True
            finally:
                """ no conn.close() since we can maintain connection
                 during life of our application"""
                pass

        if database_type in self._available_database_type:
            if database_type == 'MySQL':
                if self._mysql_conn_connection(**kwargs):
                    self.which_database = 'MySQL'
                    return True
                return False
        else:
            logger.info('This db has not yet implemented')
            print('This db has not yet implemented')
            return True

    def _mysql_conn_connection(self, **kwargs) -> bool:
        try:
            self._conn = pymysql.connect(host=kwargs.get('host'),
                                         user=kwargs.get('user'),
                                         passwd=kwargs.get('password'),
                                         db=kwargs.get('database'),
                                         port=int(kwargs.get('port')),
                                         charset="utf8")

            self._cursor = self._conn.cursor()
            return True

        except Exception as e:
            logger.info('Cannot connect to {}. '
                        'User={}. '
                        'Pass={}. '
                        'Port={}. '
                        'Db={}. '.format(
                            kwargs.get('host'),
                            kwargs.get('user'),
                            kwargs.get('password'),
                            kwargs.get('port'),
                            kwargs.get('database')
                        )
                        )
            print(e)
            return False
