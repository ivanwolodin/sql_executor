import random
import sqlite3
import time


class DataBase:
    """ creates db and fills it with values"""
    def __init__(self):
        self._conn = None
        self._cursor = None
        self._available_actions = ['SELECT', 'UPDATE', 'INSERT', 'DELETE', ]
        self._currently_not_available = ['CREATE', 'DROP', ]

    @staticmethod
    def str_time_prop(start, end, format, prop):
        stime = time.mktime(time.strptime(start, format))
        etime = time.mktime(time.strptime(end, format))

        ptime = stime + prop * (etime - stime)
        return time.strftime(format, time.localtime(ptime))

    @staticmethod
    def random_date(start, end, prop):
        return DataBase.str_time_prop(start, end, '%Y.%m.%d', prop)

    def create_db_in_memory(self) -> bool:
        """
        create simple table stocks

        production_date | status | salesman | real_value | price

        :return:
        """
        try:
            self._conn = sqlite3.connect("file::memory:?cache=shared", uri=True)
            self._cursor = self._conn.cursor()

            self._cursor.execute('''CREATE TABLE stocks
                         (production_date text, status text, salesman text, real_value int, price real)''')

            for i in range(10):
                self._cursor.execute("INSERT INTO"
                                    " stocks"
                                    " VALUES ""('{}', '{}','{}',{}, {})".format(DataBase.random_date("2008.01.01", "2020.01.01", random.random()),
                                                                                 random.choice(['Buy', 'Sell', 'Rent']),
                                                                                 random.choice(['Lisa', 'John', 'Hanna', 'Peter', 'Alice']),
                                                                                 random.randint(10, 100),
                                                                                 round(random.uniform(30, 200), 2)
                                                                                )
                                     )
            self._conn.commit()
        except Exception as e:
            self._conn.close()
            # print(e)
            return False
        else:
            return True
        finally:
            """ no conn.close() since we use :memory: and need to maintain connection during life of our application"""
            pass

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
                return self._cursor.fetchall()

        except Exception as e:
            print(e)
            return [['Check sql syntax or no such table']]

    def _validate_sql(self, sql_statement):
        try:
            action = sql_statement.split(' ', 1)[0]
        except Exception as e:
            return False, [['Cannot validate sql']]

        if action not in self._available_actions:
            if action in self._currently_not_available:
                return False, [['Action {} is not implemented yet'.format(action)]]

            if action not in self._currently_not_available:
                return False, [['Check sql syntax']]

        return True, action
