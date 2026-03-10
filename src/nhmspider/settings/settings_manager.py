from nhmspider.exceptions import SettingsTypeError


class SettingsManager(dict):
    """
    配置管理
    """

    def get_integer(self, key, default=None) -> int:
        try:
            return int(self.get(key, default or 0))
        except TypeError:
            raise SettingsTypeError(key, self[key], int)

    get_int = get_integer

    def get_float(self, key, default=None) -> float:
        try:
            return float(self.get(key, default or 0.0))
        except TypeError:
            raise SettingsTypeError(key, self[key], float)

    def get_dictionary(self, key, default=None) -> dict:
        try:
            return dict(self.get(key, default or {}))
        except TypeError:
            raise SettingsTypeError(key, self[key], dict)

    get_dict = get_dictionary

    def get_boolean(self, key, default=None) -> bool:
        try:
            return bool(self.get(key, default or False))
        except TypeError:
            raise SettingsTypeError(key, self[key], dict)

    get_bool = get_boolean

    def get_string(self, key, default=None) -> str:
        try:
            return str(self.get(key, default or ""))
        except TypeError:
            raise SettingsTypeError(key, self[key], dict)

    get_str = get_string

    def get_list(self, key, default=None) -> list:
        try:
            return list(self.get(key, default or []))
        except TypeError:
            raise SettingsTypeError(key, self[key], list)

    def __or__(self, other):
        return SettingsManager(super().__or__(other))

    def __ior__(self, other):
        return super().__ior__(other)

    def __str__(self):
        return f"<{self.__class__.__name__} {super().__str__()}>"

    def update(self, __m, **kwargs) -> None:
        return super().update(__m)
