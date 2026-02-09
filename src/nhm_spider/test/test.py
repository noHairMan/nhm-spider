import asyncio

import aiohttp
from aiohttp_socks import ProxyConnector


async def main():
    connector = ProxyConnector.from_url("socks5://10.11.35.215:9050")
    async with aiohttp.ClientSession(
        connector=connector,
    ) as session:
        response = await session.get("https://www.google.com")
        print(response.status)
        print(await response.text())


if __name__ == "__main__":
    asyncio.run(main())
