class Headers(dict):
    def __init__(self, header: dict):
        super().__init__({key.title(): header[key] for key in header} if header else {})

    def __getitem__(self, key):
        return super().__getitem__(key.title())

    def __setitem__(self, key, value):
        super().__setitem__(key.title(), str(value))

    def __or__(self, other):
        t = super().__or__(other)
        for k in other:
            if other[k] is None:
                del t[k]
        return t

    def __ior__(self, other):
        t = super().__ior__(other)
        for k in other:
            if other[k] is None:
                del t[k]
        return t
