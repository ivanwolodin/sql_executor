import random
import re
import sqlite3
import time

import pymysql
import pandas as pd

from logger import logger


class DataBase:
    """ creates db and fills it with values"""
    _available_database_type = ['MySQL', ]
    _available_actions = ['SELECT', 'UPDATE', 'INSERT', 'DELETE', ]
    _currently_not_available = ['CREATE', 'DROP', ]

    def __init__(self, which_database=':memory:'):
        self._conn = None
        self._cursor = None

        self.which_database = ''

        status, self.which_database = self.capture_database(
            database_type=which_database)
        if not status:
            logger.critical('Cannot capture database')
        number_of_raws = 10

        if self.create_db_in_memory(number_of_rows=10) != number_of_raws:
            logger.critical('Inconsistent data in db')

    @staticmethod
    def str_time_prop(start, end, format, prop):
        stime = time.mktime(time.strptime(start, format))
        etime = time.mktime(time.strptime(end, format))

        ptime = stime + prop * (etime - stime)
        return time.strftime(format, time.localtime(ptime))

    @staticmethod
    def random_date(start, end, prop):
        return DataBase.str_time_prop(start, end, '%Y.%m.%d', prop)

    @staticmethod
    def _remove_comments(string):
        string = re.sub(re.compile(r'/\*.*?\*/', re.DOTALL), '', string)
        string = string.lstrip(' ')
        return string

    def create_db_in_memory(
            self,
            number_of_rows=10,
            table_name='stocks') -> int:
        """
        create simple table stocks

        production_date | status | salesman | real_value | price

        :return:
        """

        self._cursor.execute('CREATE TABLE {} '
                             '(production_date text,'
                             ' status text,'
                             ' salesman text,'
                             ' real_value int,'
                             ' price real)'.format(table_name))
        raws_to_insert = [
            (DataBase.random_date('2008.01.01', '2020.01.01', random.random()),
             random.choice(['Buy', 'Sell', 'Rent']),
             random.choice(['Lisa', 'John', 'Hanna', 'Peter', 'Alice']),
             random.randint(10, 100),
             round(random.uniform(30, 200), 2))
            for _ in range(number_of_rows)
        ]

        self._cursor.executemany(
            'INSERT INTO {} VALUES (?,?,?,?,?)'.format(table_name),
            raws_to_insert)

        self._conn.commit()
        return self._cursor.rowcount

    def execute_sql(self, sql_statement):

        status, action = self._validate_sql(sql_statement=sql_statement)
        if not status:
            return pd.DataFrame(action, columns=['error'])

        try:
            if action in ['UPDATE', 'INSERT', 'DELETE']:
                self._cursor.execute(sql_statement)
                self._conn.commit()
                if self._cursor.rowcount >= 1:
                    return pd.DataFrame(
                        [
                            ['{} rows affected'.format(self._cursor.rowcount)]
                        ], columns=['result'])
                else:
                    return pd.DataFrame(
                        [['Zero rows affected!']], columns=['error'])
            else:
                self._cursor.execute(sql_statement)
                res = self._cursor.fetchall()
                if not res:
                    return pd.DataFrame(
                        [['empty response, no such data']], columns=['error'])
                colnames = self._cursor.description
                data = pd.DataFrame(
                    res, columns=[
                        elem[0] for elem in colnames])
                data.index += 1
                return data

        except Exception as e:
            print(e)
            logger.info(
                'Wrong sql syntax or missing table.'
                ' sql_statement={}.'
                ' Error: {}'.format(sql_statement, e))
            return pd.DataFrame(
                [['Check sql syntax or no such table']], columns=['error'])

    def _validate_sql(self, sql_statement):
        sql_statement = DataBase._remove_comments(sql_statement)
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

    def capture_database(self, database_type=':memory:', **kwargs):
        if database_type == self.which_database:
            # print('Db has not changed')
            return True, self.which_database

        if database_type == ':memory:':
            try:
                self._conn = sqlite3.connect(
                    'file::memory:?cache=shared', uri=True)
                self._cursor = self._conn.cursor()

            except Exception as e:
                self._conn.close()
                logger.error('Cannot connect to :memory: Error: {}'.format(e))
                print(e)
                return False, self.which_database
            else:
                self.which_database = ':memory:'
                return True, self.which_database
            finally:
                """ no conn.close() since we can maintain connection
                 during life of our application"""
                pass

        if database_type in self._available_database_type:
            if database_type == 'MySQL':
                if self._mysql_conn_connection(**kwargs):
                    self.which_database = 'MySQL'
                    return True, self.which_database
                return False, self.which_database
        else:
            logger.info('This db has not yet implemented')
            print('This db has not yet implemented')
            return False, self.which_database

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
