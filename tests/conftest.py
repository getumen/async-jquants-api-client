from collections.abc import AsyncGenerator

import pytest

from async_jquants_api_client import JQuantsClientV2


@pytest.fixture
async def client() -> AsyncGenerator[JQuantsClientV2, None]:
    async with JQuantsClientV2(api_key="dummy_api_key") as client:
        yield client
