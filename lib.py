lower_border_4_byte = - (2 ** 31)
high_border_4_byte = 2 ** 31 - 1

def is_int_4_byte(num : int) -> bool:
    '''возвращает True, если это 4-байтный int'''
    return isinstance(num, int) and \
           lower_border_4_byte <= num <= high_border_4_byte

def is_int_2_byte(num: int) -> bool:
    pass
class StrictDict(dict):
    '''словарь, в который нельзя добавлять ключи'''
    def __setitem__(self, key, value):
        if key not in self:
            raise KeyError("{} is illegal key".format(repr(key)))
        dict.__setitem__(self, key, value)

# print(is_int_4_byte('fdsa'))
