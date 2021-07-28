from nhm_spider.settings import default_settings
from nhm_spider.settings.settings_manager import SettingsManager


def get_default_settings():
    settings = {}
    for key in dir(default_settings):
        if key.isupper():
            settings[key] = getattr(default_settings, key)
    return SettingsManager(settings)
