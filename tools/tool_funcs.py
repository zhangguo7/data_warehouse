
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
		
def other2str(other):
    """其他类型转int类型

    :param other: 其他类型转int类型，不能转换则指定一个默认值
    :param except_value: 不能转换返回的默认值
    :return: int类型的value
    """
    try:
        return str(other)
    except:
        return ''


def angle2half(string):
    """全角字符转半角字符
    
    :param string: 原始字符串
    :return: 转换后的字符串
    """
    angles = 'ｚｘｃｖｂｎｍａｓｄｆｇｈｊｋｌｑｗｅｒｔｙｕｉｏｐＺＸＣＶＢＮＭＡＳＤＦＧＨＪＫＬＱＷＥＲＴＹＵＩＯＰ１２３４５６７８９０'
    halfs = 'zxcvbnmasdfghjklqwertyuiopZXCVBNMASDFGHJKLQWERTYUIOP1234567890'
    for angle,half in zip(angles,halfs):
        string= string.replace(angle,half)
    return string