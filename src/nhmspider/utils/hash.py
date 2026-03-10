import hashlib

import ujson


def hash_dictionary(mapping: dict) -> str:
    """
    为字典生成一个SHA256哈希值，用于缓存不同配置的浏览器实例。

    Args:
        mapping: 要哈希的字典

    Returns:
        16进制的SHA256哈希字符串
    """
    # 将字典转换为JSON字符串，确保一致的序列化顺序
    json_str = ujson.dumps(mapping, sort_keys=True, separators=(",", ":"))
    # 生成SHA256哈希
    hash_obj = hashlib.sha256(json_str.encode("utf-8"))
    return hash_obj.hexdigest()
