from nhm_spider.settings import default_settings


def get_default_settings():
    settings = {}
    for key in dir(default_settings):
        if key.isupper():
            settings[key] = getattr(default_settings, key)
    return settings
