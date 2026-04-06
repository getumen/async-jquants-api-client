from collections.abc import AsyncGenerator

import pytest

from async_jquants_api_client import JQuantsClient


@pytest.fixture
async def client() -> AsyncGenerator[JQuantsClient, None]:
    async with JQuantsClient(api_key="dummy_api_key") as client:
        yield client
