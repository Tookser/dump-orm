#!/usr/bin/env python3
import abc
from collections import namedtuple
# import pdb


from lib import is_int_4_byte, StrictDict
import dblib
from dblib import field_for_db

from fields import AbstractField, IntegerField, TextField

db = dblib.DBWrapper()

class DBException(Exception):
    pass


class MetaTable(type):
    def _add_field(self, key, value, fields):
        '''управляет созданием поля value в таблице под именем key
        вернее, в списке fields'''
        self._names.append(key)
        self._types_of_values[key] = value.__class__._type

        if value.pk:
            if self.pk_flag:
                raise DBException('No more than one primary key')
            else:
                self.pk_flag = True
                self.pk_key_name = key
        fields.append(field_for_db(key,
                                   value.__class__.type(),
                                   value.pk))
        value.name = key # нужно, чтобы переменная знала своё имя?

    def __init__(self, name, bases, attrs):
        super().__init__(name, bases, attrs)

        if name != 'Table': # TODO сделать константу
            # словарь из строк с типами ячеек (int, text и т.д.)
            self._types_of_values = {}

            # список имён полей, для каждой таблицы свой
            self._names = []

            # field_for_db = namedtuple('field_for_db', 'name type pk')

            # список из field_for_db
            fields = []

            self.pk_flag = False # флаг показывает, есть ли уже primary key
            self.pk_key_name = None # его название
            for key, value in attrs.items():
                if isinstance(value, AbstractField):
                    self._add_field(key, value, fields)
            if not fields:
                raise DBException('Can\'t create type of objects \
                                  without fields')
            # for field in fields:

            try:
                db.create_table(name, fields, self.pk_key_name)
            except dblib.QueryException:
                raise DBException('Can\'t create table, internal error')


class Table(metaclass = MetaTable):
    def __init__(self, *args, _save=True, **kwargs):
        '''создаёт объект
        _save - если нужно сохранять
        при false обращение по id'у'''
        self._values = {}

        if bool(args) and bool(kwargs):
            raise ValueError('Only positional or only keyword arguments')

        elif args or kwargs:
            if args:
                # print(self._names, args)
                if len(self._names) == len(args):
                    for i, name in enumerate(self._names):
                        setattr(self, name, args[i])
                else:
                    raise ValueError('Not enough positional arguments')
            elif kwargs:
                for key in kwargs:
                    if key in self._names:
                        setattr(self, key, kwargs[key])
                        # print('set %s' % key)
                    else:
                        raise KeyError('There are no key {} in table {}'.\
                                        format(key, self.__class__.__name__))
            else:
                raise ValueError('Unknown Error')

            if _save:
                self.save()
            else:
                pass # TODO понять, как тут лучше
                # raise NotImplementedError
                # self._update()
        else:
            raise ValueError('Empty record')



    def save(self):
        '''сохранение при создании объекта'''
        db.make_record(table=self.__class__.__name__, content=iter(self))

    def update(self):
        '''обновление объекта в БД'''
        # raise NotImplementedError
        db.update_record(table=self.__class__.__name__, content=self._values)

    def delete(self):
        '''удаляет объект'''
        # raise NotImplementedError
        # TODO сделать
        db.delete_record(table=self.__class__.__name__,
                         pk=self.pk_key_name,
                         pk_value=getattr(self, self.pk_key_name))

    @classmethod
    def _from_record_constructor(cls, record):
        '''конструирует объект класса cls из tupl-а record'''
        return cls(_save=False, *record)

    @classmethod
    def _sql_cursor_to_list(cls, cursor):
        '''Превращает курсор в список объектов'''
        cursor = list(cursor)
        cursor = [cls._from_record_constructor(record) for record in cursor]
        return cursor

    @classmethod
    def all(cls):
        '''возвращает список из всех объектов'''
        return cls._sql_cursor_to_list(db.select_all(table=cls.__name__))

    def __iter__(self):
        '''возвращает итератор по полям объекта'''
        # TODO переписать чтобы нормально перебирало поля, а не это вот
        return iter(self._values.items())

    def __str__(self):
        '''для удобного вывода'''
        s = f'''Object of type {self.__class__.__name__}:\n'''
        for name, value in self:
            s += f'{name:10} | {value:15}\n'
        return s

    def __repr__(self):
        '''для нормального отображения в списках'''
        s = self.__class__.__name__
        l = []
        for name, value in self:
            l.append('{}={}'.format(name, value))
        s += '(' + ', '.join(l) + ')'

        return s

def main():
    class MyNiceUser(Table):
        age = IntegerField()
        height = IntegerField()
        name = TextField(pk=True)

    t = MyNiceUser(40, 200, 'Basketbolist')
    t2 = MyNiceUser(42, 150, 'Basketbolist2')
    t.delete()
    db.debug_print('MyNiceUser')

def main1():
    class MyNiceUser(Table):
        age = IntegerField()
        height = IntegerField()
        name = TextField(pk=True)
        # name = TextField()

    # class MyDeusDevs(Table):
    #     age = IntegerField()
    #     exp = IntegerField()
    #     name = TextField()

    t = MyNiceUser(age = 24, height = 185, name = 'John')
    # t2 = MyNiceUser(age = 125, height = 90, name = 'Kin')

    # t3 = MyDeusDevs(age=40, exp=45, name='Ken')
    # t4 = MyDeusDevs(50, 10, 'Jun')
    # try:
    #     pass
    #     # t5 = MyDeusDevs(500)
    # except ValueError:
    #     pass
    # else:
    #     raise
    t.age = t.age * 2
    t.update()
    db.debug_print('MyNiceUser')
    print('All output')
    print(t.all())
    print(t.all())
    print(t.all())
    db.debug_print('MyNiceUser')
    t.delete()
    db.debug_print('MyNiceUser')
    # db.debug_print('MyDeusDevs')

if __name__ == '__main__':
    main()

# print(IntegerField.type())
# print(t)
# print(t2)
# for i in t:
#     print('hello:', i)
# for i in t2:
#     print('hello 2:', i)
# # t = MyNiceUser(10, 100)
