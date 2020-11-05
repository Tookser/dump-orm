lower_border_4_byte = - (2 ** 31)
high_border_4_byte = 2 ** 31 - 1

def is_int_4_byte(num : int) -> bool:
    '''возвращает True, если это 4-байтный int'''
    return isinstance(num, int) and \
           lower_border_4_byte <= num <= high_border_4_byte


# print(is_int_4_byte('fdsa'))
