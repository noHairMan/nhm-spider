class Headers(dict):
    def __init__(self, header: dict):
        super(Headers, self).__init__({key.title(): header[key] for key in header} if header else {})

    def __getitem__(self, key):
        return super(Headers, self).__getitem__(key.title())

    def __setitem__(self, key, value):
        super(Headers, self).__setitem__(key.title(), str(value))

    def __or__(self, other):
        t = super(Headers, self).__or__(other)
        for k in other:
            if other[k] is None:
                del t[k]
        return t

    def __ior__(self, other):
        t = super(Headers, self).__ior__(other)
        for k in other:
            if other[k] is None:
                del t[k]
        return t
