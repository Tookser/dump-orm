#!/usr/bin/env python3
import abc
from collections import namedtuple
# import pdb


from lib import is_int_4_byte
import dblib
from dblib import field_for_db

db = dblib.DBWrapper()

class DBException(Exception):
    pass

class AbstractField(abc.ABC):
    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        self._no_name = True # флаг, говорящий, что пока нет имени у колонки
        self._type = None
        self._primary_key = kwargs.get('pk', False)

    def __get__(self, instance, owner_class):
        return instance._values[self.name]

    @abc.abstractmethod
    def __set__(self, instance, value):
        '''обязательно реализуется в подклассах,
           т.к. нужна валидация'''
        pass

    @property
    def name(self):
        '''возвращает имя колонки'''
        return self._name

    @property
    def pk(self):
        return self._primary_key

    @classmethod
    def type(cls):
        if cls._type is not None:
            return cls._type
        else:
            raise AttributeError('There is no type')

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            if self._no_name:
                self._name = value
                self._no_name = False
            else:
                raise ValueError('You\'ve already set the name of field')
        else:
            raise ValueError('Field name should be str')

class IntegerField(AbstractField):
    _type = 'int'
    '''целочисленное поле'''
    def __init__(self, *args, **kwargs): #size=4):
        # TODO реализовать разные размеры
        super().__init__(*args, **kwargs)

    # def __str__(self):
        # return f'{self.name} int'

    def __set__(self, instance, value):
        # print('SET INT')
        if is_int_4_byte(value):
            instance._values[self.name] = value
        else:
            raise ValueError(f'You should input int4, \
                               but your entered "{value}"')

class TextField(AbstractField):
    '''текстовое поле'''
    _type = 'text'

    def __init__(self, *args, **kwargs): #size=4):
        # TODO разные размеры
        super().__init__(*args, **kwargs)

    # def __str__(self):
    #     return f'{self.name} int'

    def __set__(self, instance, value):
        # print('SET STR')
        if isinstance(value, str):
            instance._values[self.name] = value
        else:
            raise ValueError(f'You should input string, \
                               but your entered "{value}"')


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
                pk_key_name = key
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
            pk_key_name = None # его название
            for key, value in attrs.items():
                if isinstance(value, AbstractField):
                    self._add_field(key, value, fields)
                    # self._names.append(key)
                    # self._types_of_values[key] = value.__class__._type

                    # if value.pk:
                    #     if self.pk_flag:
                    #         raise DBException('No more than one primary key')
                    #     else:
                    #         self.pk_flag = True
                    #         pk_key_name = key
                    # fields.append(field_for_db(key,
                    #                            value.__class__.type(),
                    #                            value.pk))
                    # value.name = key # нужно, чтобы переменная знала своё имя?
            if not fields:
                raise DBException('Can\'t create type of objects \
                                  without fields')
# нужно всегда иметь поле-праймари кей TODO сделать это
            try:
                db.create_table(name, fields, pk_key_name)
            except dblib.QueryException:
                raise DBException('Can\'t create table, internal error')


class Table(metaclass = MetaTable):
    def __init__(self, *args, **kwargs):
        self._values = {}

        if bool(args) and bool(kwargs):
            raise ValueError('Only positional or only keyword arguments')
        if args:
            pass #TODO сделать
        elif kwargs:
            for key in kwargs:
                if key in self._names:
                    setattr(self, key, kwargs[key])
                    # print('set %s' % key)
                else:
                    raise KeyError('There are no key {} in table {}'.\
                                    format(key, self.__class__.__name__))
            self.save()
        else:
            raise ValueError('Empty record')



    def save(self):
        db.make_record(table=self.__class__.__name__, content=iter(self))

    @classmethod
    def all(cls):
        # TODO реализовать, предварительно сделав специальный конструктор
        raise NotImplementedError

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

def main():
    class MyNiceUser(Table):
        age = IntegerField()
        height = IntegerField()
        # name = TextField(pk=True)
        name = TextField()

    class MyDeusDevs(Table):
        age = IntegerField()
        exp = IntegerField()
        name = TextField()

    t = MyNiceUser(age = 24, height = 185, name = 'John')
    t2 = MyNiceUser(age = 125, height = 90, name = 'Kin')

    t3 = MyDeusDevs(age=40, exp=45, name='Ken')

    db.debug_print('MyNiceUser')
    db.debug_print('MyDeusDevs')

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
