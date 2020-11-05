from collections import namedtuple
import sqlite3


# ЗДЕСЬ ДАННЫЕ ДЛЯ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ
# TODO сделать возможность их менять

field_for_db = namedtuple('field_for_db', 'name type pk')


class QueryException(Exception):
    '''исключение кидается при ошибках, связанных с запросами
       возможно, будут подклассы'''
    pass


class DBWrapper:
    def __init__(self, file_path = None):
        if file_path is None:
            self._conn = sqlite3.connect(':memory:')
        else:
            raise NotImplementedError

    def query(self, s):
        c = self._conn.cursor()
        print('QUERY!', s)
        t = c.execute(s)
        self._conn.commit()
        return t

    def create_table(self, name, fields, primary_key):
        '''запрос без обратной связи'''
        def is_no_id_field(fields):
            '''проверяет, есть ли поле с именем id'''
            return not any(field.name == 'id' for field in fields)


        s = f'CREATE TABLE {name} ('

        if primary_key is None: # если нет pk
            if is_no_id_field(fields):
                from __init__ import IntegerField
                fields.append(field_for_db('id', IntegerField._type, True))
                primary_key = 'id'
            else:
                raise QueryException("There is field 'id' in the table")


        s += ', '.join([f'{field.name} {field.type}'
                        for field in fields]
                        +
                        ([f'PRIMARY KEY({primary_key})']
                          if primary_key else []))
        s += ');'
        # print(s)
        self.query(s)


    def make_record(self, *, table, content):
        keys = []
        values = []

        for key, value in content:
            keys.append(key)
            values.append(value)

        keys = list(map(lambda x: str(x), keys))

        values = [f"'{value}'" if isinstance(value, str) else str(value)
                  for value in values]
        s = (f'INSERT INTO {table} '
             '(' + ', '.join(keys) + ') '
             'VALUES '
             '(' + ', '.join(values) + ');')
        self.query(s)

    def debug_print(self, table):
        # TODO убрать потом
        print('Start of debug print')
        for s in self.query(f'SELECT * FROM {table}'):
            print(s)
        print('End of debug print')
