import asyncio
import logging
from typing import Optional
import aiohttp


async def save_external_links(
        urls: list[str],
        *,
        parallel: int = 5,
        logger: Optional[logging.Logger] = None) -> None:
    logger = logger or logging.getLogger(__name__)
    # semaphore
    semaphore = asyncio.Semaphore(parallel)

    # parallel requests
    async def _parallel_request(
            session: aiohttp.ClientSession,
            index: int,
            url: str,
            logger: logging.Logger) -> None:
        async with semaphore:
            await _request(session, index, url, logger)

    async with aiohttp.ClientSession() as session:
        tasks = [
                _parallel_request(session, i, url, logger)
                for i, url in enumerate(urls)]
        await asyncio.gather(*tasks)


async def _request(
        session: aiohttp.ClientSession,
        index: int,
        url: str,
        logger: logging.Logger) -> None:
    logger.debug(f'request({index}): url={url}')
    # request
    async with session.get(url) as response:
        logger.debug(f'request({index}): status={response.status}')
