from collections import namedtuple
import sqlite3

from fields import IntegerField

CAN_DELETE_NON_EXISTING = True

# ЗДЕСЬ ДАННЫЕ ДЛЯ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ
# TODO сделать возможность их менять

field_for_db = namedtuple('field_for_db', 'name type pk')


class QueryException(Exception):
    '''исключение кидается при ошибках, связанных с запросами
       возможно, будут подклассы'''
    pass

def quote(value):
    '''ставит величину в кавычки или нет,
    в зависимости от того, строка это или число
    нужно для корректной работы id=2, name='John' '''
    if isinstance(value, int):
        return value
    else:
        return f"'{value}'"

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

    def get_pk_name(self, table):
        '''возвращает имя primary key
        pk только один должен быть, иначе исключение'''
        pk = None
        for s in self.query('PRAGMA table_info(' + table + ')'):
            if s[-1] == 1: # если столбец - primary key
                if pk == None:
                    pk = s[1] # имя столбца
                else:
                    raise DBException('More than one primary key in table')
        if pk is None:
            raise DBException('No primary key in table')

        return pk


    def create_table(self, name, fields, primary_key):
        '''запрос без обратной связи'''
        def is_no_id_field(fields):
            '''проверяет, есть ли поле с именем id'''
            return not any(field.name == 'id' for field in fields)


        s = f'CREATE TABLE {name} ('

        if primary_key is None: # если нет pk
            if is_no_id_field(fields): # если нет поля под именем 'id'
                fields.append(field_for_db('id', IntegerField._type, True))
                primary_key = 'id'
            else:
                raise QueryException("There is already field 'id' in the table")


        s += ', '.join([f'{field.name} {field.type}'
                        for field in fields]
                        +
                        ([f'PRIMARY KEY({primary_key})']
                          if primary_key else []))
        s += ');'
        # print(s)
        self.query(s)


    def make_record(self, *, table, content):
        '''вставляет запись'''
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

    def update_record(self, *, table, content):
        '''обновляет запись в базе данных'''


        pk = self.get_pk_name(table)
        q = self.query('SELECT * FROM ' + table + \
                   ' WHERE ' + pk + '=\'' + content[pk] + '\';')

        print('update will update the followings:')
        for s in q:
            print('RECORD:')
            print(s)
        print('end')

        print('content:', content)
        def form(content):
            '''формирует часть запроса'''
            l = []
            for k, v in content.items():
                # v = quote(v)
                l.append(f'{k}={quote(v)}')

            return ', '.join(l)

        q = self.query('UPDATE ' + table + ' SET ' + form(content) + \
                   ' WHERE ' + pk + '=' + quote(content[pk]) + ';')

    def select_all(self, table):
        '''выбирает все записи из таблицы'''
        q = self.query('SELECT * FROM ' + table)
        return q

    def delete_record(self, *, table, pk, pk_value):
        '''удаляет запись из таблицы table с pk=pk_value'''
        q = self.query('DELETE FROM {} WHERE {}={}'.format(table, pk, quote(pk_value)))
        # l = len(q)
        # if l > 1:
        #     raise QueryException('Unknown error, two strings on one pk')
        # elif l == 0:
        #     if not CAN_DELETE_NON_EXISTING:
        #         raise QueryException('Deleting of non-existing record')

    def debug_print(self, table):
        print('Start of debug print')
        print(self.get_pk_name(table))
        # for s in self.query('PRAGMA table_info(' + table + ')'):
        #     print(s)
        for s in self.query(f'SELECT * FROM {table}'):
            print(s)
        print('End of debug print')
