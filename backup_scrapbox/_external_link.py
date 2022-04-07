import asyncio
import dataclasses
import logging
from typing import Literal, Optional
import aiohttp


@dataclasses.dataclass
class ResponseLog:
    status_code: int
    content_type: Optional[str]


@dataclasses.dataclass
class ExternalLinkLog:
    url: str
    response: Literal['error'] | ResponseLog


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
            logger: logging.Logger) -> ExternalLinkLog:
        async with semaphore:
            return await _request(session, index, url, logger)

    async with aiohttp.ClientSession() as session:
        tasks = [
                _parallel_request(session, i, url, logger)
                for i, url in enumerate(urls)]
        responses = await asyncio.gather(*tasks)
    print(responses)


async def _request(
        session: aiohttp.ClientSession,
        index: int,
        url: str,
        logger: logging.Logger) -> ExternalLinkLog:
    logger.debug(f'request({index}): url={url}')
    # request
    try:
        async with session.get(url) as response:
            logger.debug(f'request({index}): status={response.status}')
            response_log = ResponseLog(
                    status_code=response.status,
                    content_type=response.headers.get('content-type'))
            logger.debug(f'request({index}): response={response_log}')
            return ExternalLinkLog(
                url=url,
                response=response_log)
    except aiohttp.ClientError as error:
        logger.debug(f'request({index}): '
                     f'error={error.__class__.__name__}({error})')
        return ExternalLinkLog(
                url=url,
                response='error')
