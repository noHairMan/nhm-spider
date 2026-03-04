from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any

from nhm_spider.exceptions import ValidationError


# todo: 待增加不同类型的字段，增加字段`类型检查`或`自动转换`功能。
#       IntegerField, StringField, FloatField, JsonField ...
#       再深入可考虑`长度检查`等
class BaseField(ABC):
    def __init__(self):
        self.__value = None

    def __set__(self, instance, value):
        self.validate(value)
        self.__value = value

    def __get__(self, instance, owner):
        return self.__value

    @abstractmethod
    def validate(self, value: Any) -> bool:
        raise NotImplemented


class Field(BaseField):
    def validate(self, value: Any) -> bool:
        return True


class IntegerField(Field):
    def validate(self, value: Any):
        if not isinstance(value, int):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be an integer.")
