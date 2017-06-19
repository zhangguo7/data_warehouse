
def other2int(other):
    """其他类型转int类型

    :param other: 其他类型转int类型，不能转换则指定一个默认值
    :param except_value: 不能转换返回的默认值
    :return: int类型的value
    """
    try:
        return int(other)
    except:
        return float('nan')