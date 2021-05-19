from pprint import pformat


class Field:
    pass


class Item:
    __meta = set()

    def __init__(self, fields: dict = None, **kwargs):
        self.__values = dict()
        for attr in dir(self):
            if not attr.startswith("_") and isinstance(getattr(self, attr), Field):
                self.__meta.add(attr)
                self.__values[attr] = None

        if fields:
            for field in fields:
                if field not in self.__meta:
                    raise AttributeError(f"Class {self.__class__.__name__} not exists field [{field}].")
                self.__values[field] = fields[field]

        for field in kwargs:
            if field not in self.__meta:
                raise AttributeError(f"Class {self.__class__.__name__} not exists field [{field}].")
            self.__values[field] = kwargs[field]

    def __setitem__(self, key, value):
        if isinstance(self.__getattribute__(key), Field):
            self.__values[key] = value

    def __getitem__(self, item):
        if item in self.__meta:
            return self.__values[item]

    def __delitem__(self, key):
        del self.__values[key]

    def __setattr__(self, key, value):
        if key in self.__meta:
            raise AttributeError(f"Class {self.__class__.__name__} field {key} not enable rewrite.")
        super().__setattr__(key, value)

    def __iter__(self):
        return iter(self.__values)

    def __repr__(self):
        return f"{self.__class__.__name__}({pformat(self.__values)})"
