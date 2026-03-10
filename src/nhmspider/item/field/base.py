from abc import ABC, abstractmethod
from typing import Any

from nhmspider.exceptions import ValidationError


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
    def validate(self, value: Any) -> bool: ...


class IntegerField(Field):
    def validate(self, value: Any):
        if not isinstance(value, int):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be an integer.")


class StringField(Field):
    def validate(self, value: Any):
        if not isinstance(value, str):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be a string.")


class FloatField(Field):
    def validate(self, value: Any):
        if not isinstance(value, float):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be a float.")


class BooleanField(Field):
    def validate(self, value: Any):
        if not isinstance(value, bool):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be a boolean.")


class ListField(Field):
    def validate(self, value: Any):
        if not isinstance(value, list):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be a list.")


class DictField(Field):
    def validate(self, value: Any):
        if not isinstance(value, dict):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be a dict.")


class JsonField(Field):
    def validate(self, value: Any):
        import ujson

        try:
            ujson.dumps(value)
        except (TypeError, ValueError):
            raise ValidationError(f"The value of an {self.__class__.__name__} instance must be JSON serializable.")
