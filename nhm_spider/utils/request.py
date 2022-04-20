"""
copy from scrapy-2.5.0

"""
import hashlib
from typing import Dict, Optional, Tuple
from weakref import WeakKeyDictionary

from w3lib.url import canonicalize_url

from nhm_spider.http import Request

_fingerprint_cache: "WeakKeyDictionary[Request, Dict[Tuple[Optional[Tuple[bytes, ...]], bool], str]]"
_fingerprint_cache = WeakKeyDictionary()


def to_bytes(text, encoding=None, errors='strict'):
    """Return the binary representation of ``text``. If ``text``
    is already a bytes object, return it as-is."""
    if isinstance(text, bytes):
        return text
    if not isinstance(text, str):
        raise TypeError('to_bytes must receive a str or bytes '
                        f'object, got {type(text).__name__}')
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)


def request_fingerprint(
        request: Request,
        keep_fragments: bool = False,
):
    headers: Optional[Tuple[bytes, ...]] = None
    cache = _fingerprint_cache.setdefault(request, {})
    cache_key = (headers, keep_fragments)
    if cache_key not in cache:
        fp = hashlib.sha1()
        fp.update(to_bytes(request.method))
        fp.update(to_bytes(canonicalize_url(request.url, keep_fragments=keep_fragments)))
        fp.update(request.body or b'')
        cache[cache_key] = fp.hexdigest()
    return cache[cache_key]
