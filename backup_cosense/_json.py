from __future__ import annotations

import contextlib
import json
import logging
import pathlib
from typing import Any, Iterator, Optional

import jsonschema
import requests


def parse_json(
    text: str,
    *,
    schema: Optional[dict] = None,
) -> Optional[Any]:
    value = json.loads(text)
    # JSON Schema validation
    if schema is not None:
        jsonschema.validate(instance=value, schema=schema)
    return value


def load_json(
    path: pathlib.Path,
    *,
    schema: Optional[dict] = None,
) -> Optional[Any]:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as file:
        value = json.load(file)
    # JSON Schema validation
    if schema is not None:
        jsonschema.validate(instance=value, schema=schema)
    return value


def save_json(
    path: pathlib.Path,
    data: Any,
    *,
    schema: Optional[dict] = None,
    indent: Optional[int] = 2,
) -> None:
    # JSON Schema validation
    if schema is not None:
        jsonschema.validate(instance=data, schema=schema)
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
    with path.open(mode="w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=indent,
        )
        file.write("\n")


@contextlib.contextmanager
def with_session(session: Optional[requests.Session]) -> Iterator[requests.Session]:
    try:
        if session is None:
            temp_session = requests.Session()
            yield temp_session
        else:
            yield session
    finally:
        if session is None:
            temp_session.close()


def request_json(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: Optional[float] = None,
    schema: Optional[dict] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[Any]:
    logger = logger or logging.getLogger(__name__)
    # request
    logger.info(f"get request: {url}")
    with with_session(session) as session_:
        response = session_.get(url, timeout=timeout)
        if not response.ok:
            logger.error(f'failed to get request "{url}"')
            return None
    # jsonschema validation
    value = json.loads(response.text)
    if schema is not None:
        jsonschema.validate(instance=value, schema=schema)
    return value
