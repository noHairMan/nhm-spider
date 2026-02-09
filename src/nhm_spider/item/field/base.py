# -*- coding: utf-8 -*-
"""
    model层基类
    
    @Time : 2022/7/8 16:18
    @Author : noHairMan
    @File : base.py
    @Project : nhm-spider
"""


# todo: 待增加不同类型的字段，增加字段`类型检查`或`自动转换`功能。
#       IntegerField, StringField, FloatField, JsonField ...
#       再深入可考虑`长度检查`等
class Field:
    def __init__(self):
        pass


class IntegerField(Field):
    pass
