"""
copy from scrapy-2.5.0

"""
"""
This module provides some useful functions for working with
scrapy.http.Request objects
"""

import hashlib
from typing import Dict, Iterable, Optional, Tuple, Union
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
        include_headers: Optional[Iterable[Union[bytes, str]]] = None,
        keep_fragments: bool = False,
):
    """
    Return the request fingerprint.

    The request fingerprint is a hash that uniquely identifies the resource the
    request points to. For example, take the following two urls:

    http://www.example.com/query?id=111&cat=222
    http://www.example.com/query?cat=222&id=111

    Even though those are two different URLs both point to the same resource
    and are equivalent (i.e. they should return the same response).

    Another example are cookies used to store session ids. Suppose the
    following page is only accessible to authenticated users:

    http://www.example.com/members/offers.html

    Lot of sites use a cookie to store the session id, which adds a random
    component to the HTTP Request and thus should be ignored when calculating
    the fingerprint.

    For this reason, request headers are ignored by default when calculating
    the fingeprint. If you want to include specific headers use the
    include_headers argument, which is a list of Request headers to include.

    Also, servers usually ignore fragments in urls when handling requests,
    so they are also ignored by default when calculating the fingerprint.
    If you want to include them, set the keep_fragments argument to True
    (for instance when handling requests with a headless browser).

    """
    headers: Optional[Tuple[bytes, ...]] = None
    if include_headers:
        headers = tuple(to_bytes(h.lower()) for h in sorted(include_headers))
    cache = _fingerprint_cache.setdefault(request, {})
    cache_key = (headers, keep_fragments)
    if cache_key not in cache:
        fp = hashlib.sha1()
        fp.update(to_bytes(request.method))
        fp.update(to_bytes(canonicalize_url(request.url, keep_fragments=keep_fragments)))
        fp.update(request.body or b'')
        if headers:
            for hdr in headers:
                if hdr in request.headers:
                    fp.update(hdr)
                    for v in request.headers.getlist(hdr):
                        fp.update(v)
        cache[cache_key] = fp.hexdigest()
    return cache[cache_key]
