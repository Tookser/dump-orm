import abc
from lib import is_int_4_byte


class AbstractField(abc.ABC):  # TODO мб переименовать в Field
    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        self._no_name = True  # флаг, говорящий, что пока нет имени у колонки
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

    def __init__(self, *args, **kwargs):  # size=4):
        # TODO реализовать разные размеры
        super().__init__(*args, **kwargs)

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

    def __init__(self, *args, **kwargs):  # size=4):
        # TODO разные размеры
        super().__init__(*args, **kwargs)

    def __set__(self, instance, value):
        # print('SET STR')
        if isinstance(value, str):
            instance._values[self.name] = value
        else:
            raise ValueError(f'You should input string, \
                               but your entered "{value}"')
