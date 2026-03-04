from pprint import pformat

from nhm_spider.item.field.base import Field


class ItemMeta(type):
    def __new__(cls, name, bases, attrs):
        instance = super().__new__(cls, name, bases, attrs)
        meta = set()
        for key in attrs:
            if isinstance(attrs[key], Field):
                meta.add(key)
        instance.__meta = frozenset(meta)
        return instance

    @property
    def meta(cls):
        return cls.__meta


class BaseItem(metaclass=ItemMeta):
    def __init__(self, fields: dict = None, **kwargs):
        self._meta = self.__class__.meta
        if fields:
            for field in fields:
                if field not in self._meta:
                    raise AttributeError(
                        f"Class {self.__class__.__name__} not exists field [{field}].",
                    )
                setattr(self, field, fields[field])

        for field in kwargs:
            if field not in self.__meta:
                raise AttributeError(
                    f"Class {self.__class__.__name__} not exists field [{field}].",
                )
            setattr(self, field, kwargs[field])

    def __repr__(self):
        return f"{self.__class__.__name__}({pformat(self.to_dict())})"

    def to_dict(self):
        return {key: getattr(self, key) for key in self._meta}


class Item(BaseItem):
    pass
