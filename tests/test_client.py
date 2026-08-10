import os
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from pytest_httpx import HTTPXMock

from async_jquants_api_client import JQuantsAPIError, JQuantsAuthError, JQuantsClientV2, Plan
from async_jquants_api_client.client import _aggregate_bars_n_minute
from async_jquants_api_client.constants import (
    EDINET_CROSS_SHAREHOLDINGS_COLUMNS_V2,
    EDINET_LARGE_VOLUME_SHAREHOLDERS_COLUMNS_V2,
    EDINET_MAJOR_SHAREHOLDERS_COLUMNS_V2,
    FIN_SUMMARY_COLUMNS_V2,
    FINS_DIVIDEND_COLUMNS_V2,
)


def test_client_init(client: JQuantsClientV2) -> None:
    assert client is not None


# ------------------------------------------------------------------
# 設定読み込みの優先順位
# ------------------------------------------------------------------


def test_client_raises_without_api_key() -> None:
    with patch("async_jquants_api_client.client.os.path.isfile", return_value=False):
        with patch("async_jquants_api_client.client.os.environ.get", return_value=""):
            with pytest.raises(ValueError):
                JQuantsClientV2(plan=Plan.PREMIUM)


def test_client_api_key_from_config_file() -> None:
    # _read_config をモックして設定ファイルから api_key を取得するケースをテスト
    # JQUANTS_API_KEY 環境変数が未設定の場合は設定ファイルの値が使われる
    env = {k: v for k, v in os.environ.items() if k != "JQUANTS_API_KEY"}
    with patch(
        "async_jquants_api_client.client.JQuantsClientV2._read_config",
        return_value={"api_key": "key_from_file"},
    ):
        with patch.dict(os.environ, env, clear=True):
            client = JQuantsClientV2(plan=Plan.PREMIUM)
    assert client._api_key == "key_from_file"


def test_client_env_var_overrides_config_file() -> None:
    # JQUANTS_API_KEY 環境変数が設定ファイルより優先される
    with patch(
        "async_jquants_api_client.client.JQuantsClientV2._read_config",
        return_value={"api_key": "key_from_file"},
    ):
        with patch.dict(os.environ, {"JQUANTS_API_KEY": "key_from_env"}):
            client = JQuantsClientV2(plan=Plan.PREMIUM)
    assert client._api_key == "key_from_env"


def test_client_arg_overrides_env_var() -> None:
    # api_key を引数で渡すと環境変数より優先される
    client = JQuantsClientV2(api_key="key_from_arg", plan=Plan.PREMIUM)
    assert client._api_key == "key_from_arg"


# ------------------------------------------------------------------
# パラメータ生成テスト
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_eq_master_params(httpx_mock: HTTPXMock) -> None:
    cases = [
        ({}, {}),
        ({"code": "86970"}, {"code": "86970"}),
        ({"date": "20220101"}, {"date": "20220101"}),
        ({"code": "86970", "date": "20220101"}, {"code": "86970", "date": "20220101"}),
    ]
    for kwargs, expected_params in cases:
        httpx_mock.add_response(status_code=200, json={"data": []})
        async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
            await client.get_eq_master(**kwargs)
        request = httpx_mock.get_requests()[-1]
        actual = dict(request.url.params)
        assert actual == expected_params, f"kwargs={kwargs}: expected {expected_params}, got {actual}"


@pytest.mark.asyncio
async def test_get_eq_bars_daily_params(httpx_mock: HTTPXMock) -> None:
    cases = [
        ({}, {}),
        ({"code": "86970"}, {"code": "86970"}),
        (
            {"code": "86970", "from_yyyymmdd": "20220101"},
            {"code": "86970", "from": "20220101"},
        ),
        (
            {"code": "86970", "to_yyyymmdd": "20220131"},
            {"code": "86970", "to": "20220131"},
        ),
        (
            {"code": "86970", "from_yyyymmdd": "20220101", "to_yyyymmdd": "20220131"},
            {"code": "86970", "from": "20220101", "to": "20220131"},
        ),
        ({"date_yyyymmdd": "20220115"}, {"date": "20220115"}),
        (
            {"code": "86970", "date_yyyymmdd": "20220115"},
            {"code": "86970", "date": "20220115"},
        ),
    ]
    for kwargs, expected_params in cases:
        httpx_mock.add_response(status_code=200, json={"data": []})
        async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
            await client.get_eq_bars_daily(**kwargs)
        request = httpx_mock.get_requests()[-1]
        actual = dict(request.url.params)
        assert actual == expected_params, f"kwargs={kwargs}: expected {expected_params}, got {actual}"


# ------------------------------------------------------------------
# get_eq_bars_daily_range の日付形式
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_eq_bars_daily_range_accepts_various_date_formats(
    httpx_mock: HTTPXMock,
) -> None:
    import pandas as pd
    from dateutil import tz

    from async_jquants_api_client.client import DatetimeLike

    jst = tz.gettz("Asia/Tokyo")
    date_formats: list[tuple[DatetimeLike, DatetimeLike]] = [
        ("20200227", "20200302"),  # 8桁文字列
        ("2020-02-27", "2020-03-02"),  # ハイフン区切り文字列
        (datetime(2020, 2, 27), datetime(2020, 3, 2)),  # datetime
        (
            datetime(2020, 2, 27, tzinfo=jst),
            datetime(2020, 3, 2, tzinfo=jst),
        ),  # datetime with tz
        (pd.Timestamp("2020-02-27"), pd.Timestamp("2020-03-02")),  # pd.Timestamp
    ]
    expected_dates = {
        "2020-02-27",
        "2020-02-28",
        "2020-02-29",
        "2020-03-01",
        "2020-03-02",
    }

    for start, end in date_formats:
        # 5日分のレスポンスを登録
        for _ in range(5):
            httpx_mock.add_response(status_code=200, json={"data": []})
        async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
            await client.get_eq_bars_daily_range(start, end)
        requests = httpx_mock.get_requests()[-5:]
        actual_dates = {dict(r.url.params).get("date") for r in requests}
        assert actual_dates == expected_dates, f"format {type(start)}: expected {expected_dates}, got {actual_dates}"


@pytest.mark.asyncio
async def test_get_raises_auth_error_on_401(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=401)
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(JQuantsAuthError):
            await client._get("/some/path", {})


@pytest.mark.asyncio
async def test_get_raises_auth_error_on_403(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=403)
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(JQuantsAuthError):
            await client._get("/some/path", {})


@pytest.mark.asyncio
async def test_get_raises_api_error_on_500(httpx_mock: HTTPXMock) -> None:
    # 500 should be retried 3 times then raise
    httpx_mock.add_response(status_code=500)
    httpx_mock.add_response(status_code=500)
    httpx_mock.add_response(status_code=500)
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(JQuantsAPIError):
            await client._get("/some/path", {})


@pytest.mark.asyncio
async def test_get_retries_on_500_then_succeeds(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=500)
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        response = await client._get("/some/path", {})
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_paginate_yields_all_items_single_page(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"id": 1}, {"id": 2}]},
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        items = [item async for item in client._paginate("/some/path", {})]
    assert items == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_paginate_follows_pagination_key(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"id": 1}], "pagination_key": "page2"},
    )
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"id": 2}]},
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        items = [item async for item in client._paginate("/some/path", {})]
    assert items == [{"id": 1}, {"id": 2}]


# ------------------------------------------------------------------
# eq-master
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_eq_master_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2022-11-11",
                    "Code": "86970",
                    "CoName": "日本取引所グループ",
                    "CoNameEn": "Japan Exchange Group,Inc.",
                    "S17": "16",
                    "S17Nm": "金融（除く銀行）",
                    "S33": "7200",
                    "S33Nm": "その他金融業",
                    "ScaleCat": "TOPIX Large70",
                    "Mkt": "0111",
                    "MktNm": "プライム",
                    "Mrgn": "1",
                    "MrgnNm": "信用",
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_master()
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["Code"] == "86970"
    assert df.iloc[0]["CoName"] == "日本取引所グループ"
    assert df.iloc[0]["S33"] == "7200"
    assert df.iloc[0]["Mkt"] == "0111"


@pytest.mark.asyncio
async def test_get_eq_master_empty(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_master()
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ------------------------------------------------------------------
# eq-bars-daily
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_eq_bars_daily_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2023-03-24",
                    "Code": "86970",
                    "O": 2047.0,
                    "H": 2069.0,
                    "L": 2035.0,
                    "C": 2045.0,
                    "UL": "0",
                    "LL": "0",
                    "Vo": 2202500.0,
                    "Va": 4507051850.0,
                    "AdjFactor": 1.0,
                    "AdjO": 2047.0,
                    "AdjH": 2069.0,
                    "AdjL": 2035.0,
                    "AdjC": 2045.0,
                    "AdjVo": 2202500.0,
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_bars_daily(code="86970", date_yyyymmdd="2023-03-24")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "86970"
    assert df.iloc[0]["O"] == 2047.0
    assert df.iloc[0]["H"] == 2069.0
    assert df.iloc[0]["L"] == 2035.0
    assert df.iloc[0]["C"] == 2045.0
    assert df.iloc[0]["Date"] == pd.Timestamp("2023-03-24")


@pytest.mark.asyncio
async def test_get_eq_bars_daily_follows_pagination(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2023-03-24",
                    "Code": "86970",
                    "O": 2047.0,
                    "H": 2069.0,
                    "L": 2035.0,
                    "C": 2045.0,
                }
            ],
            "pagination_key": "value1.value2.",
        },
    )
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2023-03-24",
                    "Code": "13010",
                    "O": 100.0,
                    "H": 110.0,
                    "L": 95.0,
                    "C": 105.0,
                }
            ],
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_bars_daily(date_yyyymmdd="2023-03-24")
    assert len(df) == 2
    assert list(df["Code"]) == ["13010", "86970"]


@pytest.mark.asyncio
async def test_get_eq_bars_daily_range_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    # 2日分のリクエストに対してそれぞれ1件返す
    httpx_mock.add_response(status_code=200, json={"data": [{"Code": "1234", "Date": "2024-01-05"}]})
    httpx_mock.add_response(status_code=200, json={"data": [{"Code": "1234", "Date": "2024-01-06"}]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_bars_daily_range("20240105", "20240106")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df["Date"]) == [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-06")]


@pytest.mark.asyncio
async def test_get_eq_bars_daily_range_empty(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_bars_daily_range("20240105", "20240105")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.asyncio
async def test_get_eq_bars_daily_range_requests_all_dates(httpx_mock: HTTPXMock) -> None:
    """日付範囲内の各日付に対してデータ取得できることを確認"""
    dates = pd.date_range("20240101", periods=5, freq="D")
    for d in dates:
        httpx_mock.add_response(
            status_code=200,
            json={"data": [{"Code": "1234", "Date": d.strftime("%Y-%m-%d")}]},
        )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_eq_bars_daily_range(dates[0].strftime("%Y%m%d"), dates[-1].strftime("%Y%m%d"))
    assert len(df) == len(dates)
    requested_dates = [dict(r.url.params)["date"] for r in httpx_mock.get_requests()]
    assert requested_dates == [d.strftime("%Y-%m-%d") for d in dates]


# ------------------------------------------------------------------
# fins-summary
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fin_summary_range_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    row: dict[str, Any] = {col: None for col in FIN_SUMMARY_COLUMNS_V2}
    row["Code"] = "1234"
    row["DiscDate"] = "2024-01-05"
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_summary_range("20240105", "20240105")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "1234"


@pytest.mark.asyncio
async def test_get_fin_summary_range_uses_cache(tmp_path: Any) -> None:
    row: dict[str, Any] = {col: None for col in FIN_SUMMARY_COLUMNS_V2}
    row["Code"] = "5678"
    row["DiscDate"] = "2024-01-05"
    df_cached = pd.DataFrame([row])
    cache_dir = str(tmp_path)
    yyyy = "2024"
    os.makedirs(f"{cache_dir}/{yyyy}", exist_ok=True)
    df_cached.to_csv(f"{cache_dir}/{yyyy}/v2_fin_summary_20240105.csv.gz", index=False)

    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_summary_range("20240105", "20240105", cache_dir=cache_dir)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "5678"


@pytest.mark.asyncio
async def test_get_fin_summary_range_caches_successful_days_despite_one_failure(
    httpx_mock: HTTPXMock, tmp_path: Any
) -> None:
    """1日でも取得に失敗したら例外を送出するが、成功済みの日は例外の前にキャッシュへ
    書き込まれ、失われないことを確認する（呼び出し側が同じ cache_dir で再試行した際に
    失敗した日だけ再取得できるようにするための挙動）"""
    ok_row_1: dict[str, Any] = {col: None for col in FIN_SUMMARY_COLUMNS_V2}
    ok_row_1["Code"] = "1111"
    ok_row_1["DiscDate"] = "2024-01-03"
    ok_row_2: dict[str, Any] = {col: None for col in FIN_SUMMARY_COLUMNS_V2}
    ok_row_2["Code"] = "2222"
    ok_row_2["DiscDate"] = "2024-01-05"

    fins_summary_url = "https://api.jquants.com/v2/fins/summary"
    httpx_mock.add_response(
        status_code=200, json={"data": [ok_row_1]}, url=fins_summary_url, match_params={"date": "20240103"}
    )
    for _ in range(3):  # tenacity retries 429 up to 3 attempts before giving up
        httpx_mock.add_response(status_code=429, url=fins_summary_url, match_params={"date": "20240104"})
    httpx_mock.add_response(
        status_code=200, json={"data": [ok_row_2]}, url=fins_summary_url, match_params={"date": "20240105"}
    )

    cache_dir = str(tmp_path)
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(JQuantsAPIError):
            await client.get_fin_summary_range("20240103", "20240105", cache_dir=cache_dir)

    assert os.path.isfile(f"{cache_dir}/2024/v2_fin_summary_20240103.csv.gz")
    assert os.path.isfile(f"{cache_dir}/2024/v2_fin_summary_20240105.csv.gz")
    assert not os.path.isfile(f"{cache_dir}/2024/v2_fin_summary_20240104.csv.gz")


# ------------------------------------------------------------------
# fins-details
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fin_details_range_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    row = {
        "DiscDate": "2024-01-05",
        "DiscTime": "12:00:00",
        "Code": "1234",
        "DiscNo": "1",
        "DocType": "X",
        "FS": {},
    }
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_details_range("20240105", "20240105")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "1234"


@pytest.mark.asyncio
async def test_get_fin_details_range_uses_cache(tmp_path: Any) -> None:
    row = {
        "DiscDate": "2024-01-05",
        "DiscTime": "12:00:00",
        "Code": "5678",
        "DiscNo": "1",
        "DocType": "X",
        "FS": {"NetSales": "1000000"},
    }
    df_cached = pd.DataFrame([row])
    cache_dir = str(tmp_path)
    os.makedirs(f"{cache_dir}/2024", exist_ok=True)
    df_cached.to_parquet(f"{cache_dir}/2024/v2_fin_details_20240105.parquet", index=False)

    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_details_range("20240105", "20240105", cache_dir=cache_dir)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "5678"


@pytest.mark.asyncio
async def test_get_fin_details_range_caches_successful_days_despite_one_failure(
    httpx_mock: HTTPXMock, tmp_path: Any
) -> None:
    """1日でも取得に失敗したら例外を送出するが、成功済みの日は例外の前にキャッシュへ
    書き込まれ、失われないことを確認する（呼び出し側が同じ cache_dir で再試行した際に
    失敗した日だけ再取得できるようにするための挙動）"""
    ok_row_1 = {
        "DiscDate": "2024-01-03",
        "DiscTime": "12:00:00",
        "Code": "1111",
        "DiscNo": "1",
        "DocType": "X",
        "FS": {"NetSales": "1000000"},
    }
    ok_row_2 = {
        "DiscDate": "2024-01-05",
        "DiscTime": "12:00:00",
        "Code": "2222",
        "DiscNo": "1",
        "DocType": "X",
        "FS": {"NetSales": "2000000"},
    }

    fins_details_url = "https://api.jquants.com/v2/fins/details"
    httpx_mock.add_response(
        status_code=200, json={"data": [ok_row_1]}, url=fins_details_url, match_params={"date": "20240103"}
    )
    for _ in range(3):  # tenacity retries 429 up to 3 attempts before giving up
        httpx_mock.add_response(status_code=429, url=fins_details_url, match_params={"date": "20240104"})
    httpx_mock.add_response(
        status_code=200, json={"data": [ok_row_2]}, url=fins_details_url, match_params={"date": "20240105"}
    )

    cache_dir = str(tmp_path)
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(JQuantsAPIError):
            await client.get_fin_details_range("20240103", "20240105", cache_dir=cache_dir)

    assert os.path.isfile(f"{cache_dir}/2024/v2_fin_details_20240103.parquet")
    assert os.path.isfile(f"{cache_dir}/2024/v2_fin_details_20240105.parquet")
    assert not os.path.isfile(f"{cache_dir}/2024/v2_fin_details_20240104.parquet")


# ------------------------------------------------------------------
# fins-dividend
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fin_dividend_range_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    row: dict[str, Any] = {col: None for col in FINS_DIVIDEND_COLUMNS_V2}
    row["PubDate"] = "2024-01-05"
    row["Code"] = "1234"
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_dividend_range("20240105", "20240105")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "1234"


@pytest.mark.asyncio
async def test_get_fin_dividend_range_empty(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_dividend_range("20240105", "20240105")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ------------------------------------------------------------------
# mkt-short-ratio
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_mkt_short_ratio_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2022-10-25",
                    "S33": "0050",
                    "SellExShortVa": 1333126400.0,
                    "ShrtWithResVa": 787355200.0,
                    "ShrtNoResVa": 149084300.0,
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_mkt_short_ratio(date_yyyymmdd="2022-10-25")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["S33"] == "0050"
    assert df.iloc[0]["SellExShortVa"] == 1333126400.0
    assert df.iloc[0]["Date"] == pd.Timestamp("2022-10-25")


@pytest.mark.asyncio
async def test_get_mkt_short_ratio_range_returns_dataframe(
    httpx_mock: HTTPXMock,
) -> None:
    from async_jquants_api_client.constants import MKT_SHORT_RATIO_COLUMNS_V2

    row: dict[str, Any] = {col: None for col in MKT_SHORT_RATIO_COLUMNS_V2}
    row["Date"] = "2024-01-05"
    row["S33"] = "0050"
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_mkt_short_ratio_range("20240105", "20240105")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["S33"] == "0050"


# ------------------------------------------------------------------
# mkt-margin-alert
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_mkt_margin_alert_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "PubDate": "2024-02-08",
                    "Code": "13260",
                    "AppDate": "2024-02-07",
                    "PubReason": {
                        "Restricted": "0",
                        "DailyPublication": "0",
                        "Monitoring": "0",
                        "RestrictedByJSF": "0",
                        "PrecautionByJSF": "1",
                        "UnclearOrSecOnAlert": "0",
                    },
                    "ShrtOut": 11.0,
                    "ShrtOutChg": 0.0,
                    "ShrtOutRatio": "*",
                    "LongOut": 676.0,
                    "LongOutChg": -20.0,
                    "LongOutRatio": "*",
                    "SLRatio": 1.6,
                    "ShrtNegOut": 0.0,
                    "ShrtNegOutChg": 0.0,
                    "ShrtStdOut": 11.0,
                    "ShrtStdOutChg": 0.0,
                    "LongNegOut": 192.0,
                    "LongNegOutChg": -20.0,
                    "LongStdOut": 484.0,
                    "LongStdOutChg": 0.0,
                    "TSEMrgnRegCls": "001",
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_mkt_margin_alert()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "13260"
    assert df.iloc[0]["PubDate"] == pd.Timestamp("2024-02-08")
    assert df.iloc[0]["ShrtOut"] == 11.0
    assert df.iloc[0]["SLRatio"] == 1.6


# ------------------------------------------------------------------
# edinet-major-shareholders
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_edinet_major_shareholders_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "DocId": "S100XXXX",
                    "Code": "86970",
                    "EdinetCode": "E03814",
                    "FilerName": "株式会社日本取引所グループ",
                    "FilerNameEn": "Japan Exchange Group, Inc.",
                    "DocTypeCode": "120",
                    "SubDate": "2025-06-20",
                    "SubTime": "09:00",
                    "PerSt": "2024-04-01",
                    "PerEn": "2025-03-31",
                    "Hldrs": [
                        {
                            "Rank": 1,
                            "HldrName": "日本マスタートラスト信託銀行株式会社",
                            "HldrAddr": "東京都港区",
                            "ShsHeld": 50000000,
                            "ShsRatio": 0.15,
                        }
                    ],
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_major_shareholders(code="86970")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "86970"
    assert df.iloc[0]["SubDate"] == pd.Timestamp("2025-06-20")
    assert df.iloc[0]["Hldrs"][0]["HldrName"] == "日本マスタートラスト信託銀行株式会社"
    assert df.iloc[0]["Hldrs"][0]["Rank"] == 1
    assert dict(httpx_mock.get_requests()[0].url.params) == {"code": "86970"}


@pytest.mark.asyncio
async def test_get_edinet_major_shareholders_raises_on_edinet_code_and_code(
    httpx_mock: HTTPXMock,
) -> None:
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(ValueError, match="edinet_code"):
            await client.get_edinet_major_shareholders(edinet_code="E03814", code="86970")
    assert len(httpx_mock.get_requests()) == 0


@pytest.mark.asyncio
async def test_get_edinet_major_shareholders_returns_empty_dataframe_with_columns(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_major_shareholders(code="86970")
    assert df.empty
    assert list(df.columns) == EDINET_MAJOR_SHAREHOLDERS_COLUMNS_V2
    assert dict(httpx_mock.get_requests()[0].url.params) == {"code": "86970"}


@pytest.mark.asyncio
async def test_get_edinet_major_shareholders_range_concatenates_dates(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"DocId": "A", "Code": "86970", "SubDate": "2025-06-19", "Hldrs": []}]},
    )
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"DocId": "B", "Code": "13260", "SubDate": "2025-06-20", "Hldrs": []}]},
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_major_shareholders_range(start_dt="2025-06-19", end_dt="2025-06-20")
    assert len(df) == 2
    assert list(df["DocId"]) == ["A", "B"]
    requested_dates = {dict(r.url.params)["date"] for r in httpx_mock.get_requests()}
    assert requested_dates == {"2025-06-19", "2025-06-20"}


# ------------------------------------------------------------------
# edinet-cross-shareholdings
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_edinet_cross_shareholdings_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "DocId": "S100YYYY",
                    "Code": "86970",
                    "EdinetCode": "E03814",
                    "FilerName": "株式会社日本取引所グループ",
                    "FilerNameEn": "Japan Exchange Group, Inc.",
                    "DocTypeCode": "120",
                    "SubDate": "2025-06-20",
                    "SubTime": "09:00",
                    "PerSt": "2024-04-01",
                    "PerEn": "2025-03-31",
                    "Report": {
                        "HldrName": "株式会社日本取引所グループ",
                        "HldrCode": "86970",
                        "Spec": [
                            {
                                "IsrName": "サンプル株式会社",
                                "IsrCode": "12345",
                                "CurShs": 1000,
                                "HoldRat": 0.01,
                            }
                        ],
                        "Deem": [],
                    },
                    "Largest": {},
                    "SecondLargest": {},
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_cross_shareholdings(code="86970")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "86970"
    assert df.iloc[0]["SubDate"] == pd.Timestamp("2025-06-20")
    assert df.iloc[0]["Report"]["Spec"][0]["IsrName"] == "サンプル株式会社"
    assert dict(httpx_mock.get_requests()[0].url.params) == {"code": "86970"}


@pytest.mark.asyncio
async def test_get_edinet_cross_shareholdings_raises_on_edinet_code_and_code(
    httpx_mock: HTTPXMock,
) -> None:
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(ValueError, match="edinet_code"):
            await client.get_edinet_cross_shareholdings(edinet_code="E03814", code="86970")
    assert len(httpx_mock.get_requests()) == 0


@pytest.mark.asyncio
async def test_get_edinet_cross_shareholdings_returns_empty_dataframe_with_columns(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_cross_shareholdings(edinet_code="E03814")
    assert df.empty
    assert list(df.columns) == EDINET_CROSS_SHAREHOLDINGS_COLUMNS_V2
    assert dict(httpx_mock.get_requests()[0].url.params) == {"edinet_code": "E03814"}


@pytest.mark.asyncio
async def test_get_edinet_cross_shareholdings_range_concatenates_dates(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"DocId": "A", "Code": "86970", "SubDate": "2025-06-19"}]},
    )
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"DocId": "B", "Code": "13260", "SubDate": "2025-06-20"}]},
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_cross_shareholdings_range(start_dt="2025-06-19", end_dt="2025-06-20")
    assert len(df) == 2
    assert list(df["DocId"]) == ["A", "B"]
    requested_dates = {dict(r.url.params)["date"] for r in httpx_mock.get_requests()}
    assert requested_dates == {"2025-06-19", "2025-06-20"}


# ------------------------------------------------------------------
# edinet-large-volume-shareholders
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_edinet_large_volume_shareholders_returns_dataframe(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "DocId": "S100ZZZZ",
                    "Code": "12345",
                    "EdinetCode": "E00001",
                    "IsrName": "サンプル株式会社",
                    "DocTypeCode": "350",
                    "SubDate": "2025-06-20",
                    "SubTime": "10:00",
                    "LargeHldgTypeCode": "1",
                    "DocTitle": "大量保有報告書",
                    "ChgRsn": "0",
                    "TotalShsHeld": 1000000,
                    "TotalShsRatio": 0.05,
                    "TotalShsRatioLast": 0.04,
                    "TotalOutStks": 20000000,
                    "Hldrs": [
                        {
                            "HldrName": "サンプル投資顧問株式会社",
                            "ShsHeld": 1000000,
                            "ShsRatio": 0.05,
                            "AcqDisp": [],
                            "BrwList": [],
                            "CredList": [],
                        }
                    ],
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_large_volume_shareholders(code="12345")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "12345"
    assert df.iloc[0]["SubDate"] == pd.Timestamp("2025-06-20")
    assert df.iloc[0]["TotalShsRatio"] == 0.05
    assert df.iloc[0]["Hldrs"][0]["HldrName"] == "サンプル投資顧問株式会社"
    assert dict(httpx_mock.get_requests()[0].url.params) == {"code": "12345"}


@pytest.mark.asyncio
async def test_get_edinet_large_volume_shareholders_raises_on_edinet_code_and_code(
    httpx_mock: HTTPXMock,
) -> None:
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        with pytest.raises(ValueError, match="edinet_code"):
            await client.get_edinet_large_volume_shareholders(edinet_code="E00001", code="12345")
    assert len(httpx_mock.get_requests()) == 0


@pytest.mark.asyncio
async def test_get_edinet_large_volume_shareholders_returns_empty_dataframe_with_columns(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_large_volume_shareholders(code="12345")
    assert df.empty
    assert list(df.columns) == EDINET_LARGE_VOLUME_SHAREHOLDERS_COLUMNS_V2
    assert dict(httpx_mock.get_requests()[0].url.params) == {"code": "12345"}


@pytest.mark.asyncio
async def test_get_edinet_large_volume_shareholders_range_concatenates_dates(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"DocId": "A", "Code": "12345", "SubDate": "2025-06-19"}]},
    )
    httpx_mock.add_response(
        status_code=200,
        json={"data": [{"DocId": "B", "Code": "67890", "SubDate": "2025-06-20"}]},
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_edinet_large_volume_shareholders_range(start_dt="2025-06-19", end_dt="2025-06-20")
    assert len(df) == 2
    assert list(df["DocId"]) == ["A", "B"]
    requested_dates = {dict(r.url.params)["date"] for r in httpx_mock.get_requests()}
    assert requested_dates == {"2025-06-19", "2025-06-20"}


# ------------------------------------------------------------------
# mkt-breakdown
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_mkt_breakdown_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2015-04-01",
                    "Code": "13010",
                    "LongSellVa": 115164000.0,
                    "ShrtNoMrgnVa": 93561000.0,
                    "MrgnSellNewVa": 6412000.0,
                    "MrgnSellCloseVa": 23009000.0,
                    "LongBuyVa": 185114000.0,
                    "MrgnBuyNewVa": 35568000.0,
                    "MrgnBuyCloseVa": 17464000.0,
                    "LongSellVo": 415000.0,
                    "ShrtNoMrgnVo": 337000.0,
                    "MrgnSellNewVo": 23000.0,
                    "MrgnSellCloseVo": 83000.0,
                    "LongBuyVo": 667000.0,
                    "MrgnBuyNewVo": 128000.0,
                    "MrgnBuyCloseVo": 63000.0,
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_mkt_breakdown(date_yyyymmdd="2015-04-01")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "13010"
    assert df.iloc[0]["LongSellVa"] == 115164000.0
    assert df.iloc[0]["Date"] == pd.Timestamp("2015-04-01")


@pytest.mark.asyncio
async def test_get_mkt_breakdown_range_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2015-04-01",
                    "Code": "13010",
                    "LongSellVa": 115164000.0,
                    "ShrtNoMrgnVa": 93561000.0,
                    "MrgnSellNewVa": 6412000.0,
                    "MrgnSellCloseVa": 23009000.0,
                    "LongBuyVa": 185114000.0,
                    "MrgnBuyNewVa": 35568000.0,
                    "MrgnBuyCloseVa": 17464000.0,
                    "LongSellVo": 415000.0,
                    "ShrtNoMrgnVo": 337000.0,
                    "MrgnSellNewVo": 23000.0,
                    "MrgnSellCloseVo": 83000.0,
                    "LongBuyVo": 667000.0,
                    "MrgnBuyNewVo": 128000.0,
                    "MrgnBuyCloseVo": 63000.0,
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_mkt_breakdown_range("20150401", "20150401")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Code"] == "13010"


# ------------------------------------------------------------------
# get_list (マージロジックの確認)
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_list_merges_sector_and_market_names(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "Date": "2022-11-11",
                    "Code": "86970",
                    "CoName": "日本取引所グループ",
                    "CoNameEn": "Japan Exchange Group,Inc.",
                    "S17": "16",
                    "S17Nm": "金融（除く銀行）",
                    "S33": "7200",
                    "S33Nm": "その他金融業",
                    "ScaleCat": "TOPIX Large70",
                    "Mkt": "0111",
                    "MktNm": "プライム",
                    "Mrgn": "1",
                    "MrgnNm": "信用",
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_list()
    assert isinstance(df, pd.DataFrame)
    assert "S17NmEn" in df.columns
    assert "S33NmEn" in df.columns
    assert "MktNmEn" in df.columns
    assert df.iloc[0]["S17NmEn"] == "FINANCIALS (EX BANKS) "
    assert df.iloc[0]["S33NmEn"] == "Other Financing Business"
    assert df.iloc[0]["MktNmEn"] == "Prime"


@pytest.mark.asyncio
async def test_get_list_empty_returns_empty_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_list()
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ------------------------------------------------------------------
# fins-summary / fins-details (単体)
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fin_summary_converts_date_columns(httpx_mock: HTTPXMock) -> None:
    row: dict[str, Any] = {col: None for col in FIN_SUMMARY_COLUMNS_V2}
    row["Code"] = "86970"
    row["DiscDate"] = "2024-03-15"
    row["CurFYSt"] = "2023-04-01"
    row["CurFYEn"] = "2024-03-31"
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_summary(date_yyyymmdd="2024-03-15")
    assert df.iloc[0]["DiscDate"] == pd.Timestamp("2024-03-15")
    assert df.iloc[0]["CurFYSt"] == pd.Timestamp("2023-04-01")
    assert df.iloc[0]["CurFYEn"] == pd.Timestamp("2024-03-31")


@pytest.mark.asyncio
async def test_get_fin_summary_keeps_shareholders_equity_and_roe(httpx_mock: HTTPXMock) -> None:
    # 自己資本とROEはレスポンスの末尾に追加されたフィールド。列を固定リストで
    # 絞っているため、リストに載っていない限り黙って捨てられる。
    row: dict[str, Any] = {col: "" for col in FIN_SUMMARY_COLUMNS_V2}
    row["Code"] = "86970"
    row["DiscDate"] = "2026-05-15"
    row["Eq"] = "3074785000000"
    row["ShEq"] = "22335000000"
    row["NCShEq"] = "13432000000"
    row["ROE"] = "0.07"
    row["NCROE"] = "0.05"
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_summary(date_yyyymmdd="2026-05-15")
    assert df.iloc[0]["ShEq"] == "22335000000"
    assert df.iloc[0]["NCShEq"] == "13432000000"
    assert df.iloc[0]["ROE"] == "0.07"
    assert df.iloc[0]["NCROE"] == "0.05"
    # ShEq は Eq (非支配株主持分を含む純資産) とは別の列として残る。
    assert df.iloc[0]["Eq"] == "3074785000000"


@pytest.mark.asyncio
async def test_get_fin_details_converts_disc_date(httpx_mock: HTTPXMock) -> None:
    row = {
        "DiscDate": "2024-03-15",
        "DiscTime": "15:00:00",
        "Code": "86970",
        "DiscNo": "1",
        "DocType": "120",
        "FS": {},
    }
    httpx_mock.add_response(status_code=200, json={"data": [row]})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_fin_details(date_yyyymmdd="2024-03-15")
    assert df.iloc[0]["DiscDate"] == pd.Timestamp("2024-03-15")
    assert df.iloc[0]["Code"] == "86970"


# ------------------------------------------------------------------
# mkt-short-sale-report (複数日付列の変換)
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_mkt_short_sale_report_converts_date_columns(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {
                    "DiscDate": "2024-02-08",
                    "CalcDate": "2024-02-06",
                    "PrevRptDate": "2024-02-01",
                    "Code": "13260",
                    "ShrtSellBal": 100000.0,
                    "IssueShrs": 5000000.0,
                }
            ]
        },
    )
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_mkt_short_sale_report(disclosed_date="2024-02-08")
    assert df.iloc[0]["DiscDate"] == pd.Timestamp("2024-02-08")
    assert df.iloc[0]["CalcDate"] == pd.Timestamp("2024-02-06")
    assert df.iloc[0]["PrevRptDate"] == pd.Timestamp("2024-02-01")
    assert df.iloc[0]["Code"] == "13260"


# ------------------------------------------------------------------
# _aggregate_bars_n_minute (集約ロジック)
# ------------------------------------------------------------------


def test_aggregate_bars_n_minute_ohlcv_aggregation() -> None:
    """H=max, L=min, O=first, C=last, Vo/Va=sum の集約ルールを検証"""
    df = pd.DataFrame(
        [
            {
                "Date": "2024-01-05",
                "Time": "09:00:00",
                "Code": "86970",
                "O": 100.0,
                "H": 105.0,
                "L": 98.0,
                "C": 103.0,
                "Vo": 1000.0,
                "Va": 100000.0,
            },
            {
                "Date": "2024-01-05",
                "Time": "09:01:00",
                "Code": "86970",
                "O": 103.0,
                "H": 110.0,
                "L": 102.0,
                "C": 108.0,
                "Vo": 2000.0,
                "Va": 200000.0,
            },
            {
                "Date": "2024-01-05",
                "Time": "09:02:00",
                "Code": "86970",
                "O": 108.0,
                "H": 112.0,
                "L": 107.0,
                "C": 109.0,
                "Vo": 1500.0,
                "Va": 150000.0,
            },
            {
                "Date": "2024-01-05",
                "Time": "09:03:00",
                "Code": "86970",
                "O": 109.0,
                "H": 111.0,
                "L": 106.0,
                "C": 107.0,
                "Vo": 500.0,
                "Va": 50000.0,
            },
            {
                "Date": "2024-01-05",
                "Time": "09:04:00",
                "Code": "86970",
                "O": 107.0,
                "H": 108.0,
                "L": 105.0,
                "C": 106.0,
                "Vo": 800.0,
                "Va": 80000.0,
            },
        ]
    )
    result = _aggregate_bars_n_minute(df, n=5)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["O"] == 100.0  # 始値: 最初
    assert row["H"] == 112.0  # 高値: 最大
    assert row["L"] == 98.0  # 安値: 最小
    assert row["C"] == 106.0  # 終値: 最後
    assert row["Vo"] == 5800.0  # 出来高: 合計
    assert row["Va"] == 580000.0  # 売買代金: 合計


def test_aggregate_bars_n_minute_multiple_codes() -> None:
    """複数銘柄が独立して集約されることを確認"""
    df = pd.DataFrame(
        [
            {
                "Date": "2024-01-05",
                "Time": "09:00:00",
                "Code": "11110",
                "O": 200.0,
                "H": 210.0,
                "L": 195.0,
                "C": 205.0,
                "Vo": 300.0,
                "Va": 60000.0,
            },
            {
                "Date": "2024-01-05",
                "Time": "09:00:00",
                "Code": "22220",
                "O": 500.0,
                "H": 510.0,
                "L": 495.0,
                "C": 505.0,
                "Vo": 100.0,
                "Va": 50000.0,
            },
        ]
    )
    result = _aggregate_bars_n_minute(df, n=5)

    assert len(result) == 2
    codes = set(result["Code"])
    assert codes == {"11110", "22220"}


def test_aggregate_bars_n_minute_empty() -> None:
    df = pd.DataFrame(columns=["Date", "Time", "Code", "O", "H", "L", "C", "Vo", "Va"])
    result = _aggregate_bars_n_minute(df, n=5)
    assert result.empty


# ------------------------------------------------------------------
# bulk
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_bulk_list_returns_dataframe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=200,
        json={
            "data": [
                {"Key": "equities/master/20240101.csv.gz", "Size": 1024, "LastModified": "2024-01-02T00:00:00Z"},
                {"Key": "equities/master/20240102.csv.gz", "Size": 2048, "LastModified": "2024-01-03T00:00:00Z"},
            ]
        },
    )
    from async_jquants_api_client import BulkEndpoint

    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_bulk_list(BulkEndpoint.EQ_MASTER)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["Key", "Size", "LastModified"]
    assert len(df) == 2
    assert df.iloc[0]["Key"] == "equities/master/20240101.csv.gz"
    assert df.iloc[0]["LastModified"] == pd.Timestamp("2024-01-02T00:00:00Z")


@pytest.mark.asyncio
async def test_get_bulk_list_accepts_string_endpoint(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        df = await client.get_bulk_list("/equities/master")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    request = httpx_mock.get_requests()[-1]
    assert dict(request.url.params) == {"endpoint": "/equities/master"}


@pytest.mark.asyncio
async def test_get_bulk_list_sends_correct_endpoint_param(httpx_mock: HTTPXMock) -> None:
    from async_jquants_api_client import BulkEndpoint

    httpx_mock.add_response(status_code=200, json={"data": []})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        await client.get_bulk_list(BulkEndpoint.EQ_BARS_DAILY)
    request = httpx_mock.get_requests()[-1]
    assert dict(request.url.params) == {"endpoint": "/equities/bars/daily"}


@pytest.mark.asyncio
async def test_get_bulk_returns_url(httpx_mock: HTTPXMock) -> None:
    expected_url = "https://example.com/bulk/equities/master/20240101.csv.gz"
    httpx_mock.add_response(status_code=200, json={"url": expected_url})
    async with JQuantsClientV2(api_key="dummy", plan=Plan.PREMIUM) as client:
        url = await client.get_bulk("equities/master/20240101.csv.gz")
    assert url == expected_url
    request = httpx_mock.get_requests()[-1]
    assert dict(request.url.params) == {"key": "equities/master/20240101.csv.gz"}
