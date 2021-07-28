from nhm_spider.settings.settings_manager import SettingsManager


def test1():
    manager = SettingsManager()
    print(manager | {"1": 2})
    manager |= {"3": 3}
    # print(manager)

    manager |= SettingsManager({"5": 3})
    # print(manager)

    print(id(manager | {"2": 23}), id(manager))
    # print("dddd", {"a": 123} | manager)

    manager |= SettingsManager({"542": 3})
    print(id(manager), manager)

    # print(manager)

    manager.update({"qwe": "dgdg"})
    print(id(manager))
    # print(manager)


if __name__ == '__main__':
    test1()