from collections.abc import AsyncGenerator

import pytest

from async_jquants_api_client import JQuantsClientV2, Plan


@pytest.fixture
async def client() -> AsyncGenerator[JQuantsClientV2, None]:
    async with JQuantsClientV2(api_key="dummy_api_key", plan=Plan.PREMIUM) as client:
        yield client
