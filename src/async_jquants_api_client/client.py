import asyncio
import os
import sys
import uuid
from collections.abc import AsyncGenerator, Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypeAlias

import aiolimiter
import httpx
import pandas as pd
import tomllib
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from . import constants
from .exceptions import JQuantsAPIError, JQuantsAuthError
from .plans import BulkEndpoint, Plan

_RATE_LIMITS: dict[Plan, int] = {
    Plan.FREE: 5,
    Plan.LIGHT: 60,
    Plan.STANDARD: 120,
    Plan.PREMIUM: 500,
}

_ADDON_RATE_LIMIT = 60
_FINS_RATE_LIMIT = 60
_EDINET_RATE_LIMIT = 60

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


DatetimeLike: TypeAlias = str | date | datetime | pd.Timestamp


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUS_CODES
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError))


def _read_fin_summary_cache(path: str, date_cols: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _write_cache_atomic(path: str, write_to: Callable[[str], None]) -> None:
    """キャッシュファイルを一時ファイルへ書き込んでから `os.replace` でアトミックに配置する。

    書き込み途中でプロセスが落ちても、最終的なキャッシュファイルは「以前のまま」か
    「新しい内容に完全に置き換わった」状態のいずれかになり、書きかけの壊れたファイルが
    残らない。同期 I/O のため、呼び出し側は `asyncio.to_thread` 経由で呼ぶこと。
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp-{uuid.uuid4().hex}"
    try:
        write_to(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def _aggregate_bars_n_minute(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """1分足データをn分足に集約する"""
    if df.empty:
        return df.copy()

    df = df.copy()

    df["DateTime"] = pd.to_datetime(
        df["Date"].astype(str).str.cat(df["Time"].astype(str), sep=" "),
        errors="coerce",
    )
    df["TimeGroup"] = df["DateTime"].dt.floor(f"{n}min")

    agg_funcs = {
        "Date": "first",
        "Time": "first",
        "Code": "first",
        "O": "first",
        "H": "max",
        "L": "min",
        "C": "last",
        "Vo": "sum",
        "Va": "sum",
    }

    result = df.groupby(["Code", "TimeGroup"], as_index=False).agg(agg_funcs).drop(columns=["TimeGroup"])

    result.sort_values(["Code", "Date", "Time"], inplace=True)
    return result.reset_index(drop=True)


class JQuantsClientV2:
    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self, api_key: str | None = None, plan: Plan = Plan.FREE) -> None:
        config = self._load_config()

        if api_key is not None:
            self._api_key = api_key
        else:
            self._api_key = config.get("api_key", "")

        if not self._api_key:
            raise ValueError("api_key is required. Set it via argument, config file, or JQUANTS_API_KEY env var.")

        if plan is None:
            plan = Plan.FREE

        self._http = httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers={"x-api-key": self._api_key},
            timeout=30.0,
        )
        self._limiter = aiolimiter.AsyncLimiter(1, 60 / _RATE_LIMITS[plan])
        self._addon_limiter = aiolimiter.AsyncLimiter(1, 60 / _ADDON_RATE_LIMIT)
        self._fins_limiter = aiolimiter.AsyncLimiter(1, 60 / _FINS_RATE_LIMIT)
        self._edinet_limiter = aiolimiter.AsyncLimiter(1, 60 / _EDINET_RATE_LIMIT)

    def _is_colab(self) -> bool:
        return "google.colab" in sys.modules

    def _load_config(self) -> dict:
        config: dict = {}

        if self._is_colab():
            colab_config_path = "/content/drive/MyDrive/drive_ws/secret/jquants-api.toml"
            config = {**config, **self._read_config(colab_config_path)}

        user_config_path = f"{Path.home()}/.jquants-api/jquants-api.toml"
        config = {**config, **self._read_config(user_config_path)}

        current_config_path = "jquants-api.toml"
        config = {**config, **self._read_config(current_config_path)}

        if "JQUANTS_API_CLIENT_CONFIG_FILE" in os.environ:
            env_config_path = os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"]
            config = {**config, **self._read_config(env_config_path)}

        config["api_key"] = os.environ.get("JQUANTS_API_KEY", config.get("api_key", ""))

        return config

    def _read_config(self, config_path: str) -> dict:
        if not os.path.isfile(config_path):
            return {}

        with open(config_path, mode="rb") as f:
            ret = tomllib.load(f)

        if "jquants-api-client" not in ret:
            return {}

        return ret["jquants-api-client"]

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def _get_with_retry(
        self, path: str, params: dict, limiter: aiolimiter.AsyncLimiter | None = None
    ) -> httpx.Response:
        async with limiter or self._limiter:
            response = await self._http.get(path, params=params)
        if response.status_code in (401, 403):
            raise JQuantsAuthError(f"Authentication failed: {response.status_code}")
        if response.status_code in _RETRYABLE_STATUS_CODES:
            raise httpx.HTTPStatusError(
                f"HTTP {response.status_code}",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        return response

    async def _get(self, path: str, params: dict, limiter: aiolimiter.AsyncLimiter | None = None) -> httpx.Response:
        try:
            return await self._get_with_retry(path, params, limiter=limiter)
        except httpx.HTTPStatusError as e:
            raise JQuantsAPIError(e.response.status_code, e.response.text) from e

    async def _paginate(
        self, path: str, params: dict, limiter: aiolimiter.AsyncLimiter | None = None
    ) -> AsyncGenerator[dict, None]:
        pagination_key: str | None = None
        while True:
            current_params = params if pagination_key is None else {**params, "pagination_key": pagination_key}
            body = (await self._get(path, current_params, limiter=limiter)).json()
            for item in body["data"]:
                yield item
            pagination_key = body.get("pagination_key")
            if not pagination_key:
                break

    async def __aenter__(self) -> "JQuantsClientV2":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self._http.aclose()

    async def close(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------
    # eq-master (/equities/master)
    # ------------------------------------------------------------------
    async def get_eq_master(
        self,
        code: str = "",
        date: str = "",
    ) -> pd.DataFrame:
        """
        eq-master: 上場銘柄一覧 (v2: /equities/master)

        Args:
            code: 5桁の銘柄コード (例: 27800)。4桁指定も可能。
            date: 基準日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 上場銘柄情報
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        return pd.DataFrame([item async for item in self._paginate("/equities/master", params)])

    # ------------------------------------------------------------------
    # ユーティリティ: 業種・市場区分マスタ (v1 と同様のローカル定義)
    # ------------------------------------------------------------------
    def get_market_segments(self) -> pd.DataFrame:
        """
        市場区分コードと名称 (V2 カラム名)
        """
        df = pd.DataFrame(constants.MARKET_SEGMENT_DATA, columns=constants.MARKET_SEGMENT_COLUMNS_V2)
        df.sort_values(constants.MARKET_SEGMENT_COLUMNS_V2[0], inplace=True)
        return df

    def get_17_sectors(self) -> pd.DataFrame:
        """
        17 業種コードと名称 (V2 カラム名)
        """
        df = pd.DataFrame(constants.SECTOR_17_DATA, columns=constants.SECTOR_17_COLUMNS_V2)
        df.sort_values(constants.SECTOR_17_COLUMNS_V2[0], inplace=True)
        return df

    def get_33_sectors(self) -> pd.DataFrame:
        """
        33 業種コードと名称 (V2 カラム名)
        """
        df = pd.DataFrame(constants.SECTOR_33_DATA, columns=constants.SECTOR_33_COLUMNS_V2)
        df.sort_values(constants.SECTOR_33_COLUMNS_V2[0], inplace=True)
        return df

    # ------------------------------------------------------------------
    # get_list (v1 と同名のユーティリティ, eq-master ベース)
    # ------------------------------------------------------------------
    async def get_list(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        上場銘柄一覧 (業種・市場区分の英語名を付与したユーティリティ)

        v2 の eq-master を利用し、v2 フィールド名で返却します。

        Args:
            code: 銘柄コード (任意)
            date_yyyymmdd: 基準日 (YYYYMMDD or YYYY-MM-DD, 任意)
        Returns:
            pd.DataFrame: 上場銘柄情報 (v2 フィールド名)
        """
        df_list = await self.get_eq_master(code=code, date=date_yyyymmdd)
        if df_list.empty:
            return pd.DataFrame([], columns=constants.EQ_MASTER_COLUMNS_V2)

        # 17/33 業種 & 市場区分の英語名を付与
        df_17_sectors = self.get_17_sectors()[["S17", "S17NmEn"]]
        df_33_sectors = self.get_33_sectors()[["S33", "S33NmEn"]]
        df_segments = self.get_market_segments()[["Mkt", "MktNmEn"]]

        df_list = pd.merge(df_list, df_17_sectors, how="left", on=["S17"])
        df_list = pd.merge(df_list, df_33_sectors, how="left", on=["S33"])
        df_list = pd.merge(df_list, df_segments, how="left", on=["Mkt"])

        df_list.sort_values("Code", inplace=True)
        return df_list

    # ------------------------------------------------------------------
    # eq-bars-daily (/equities/bars/daily)
    # ------------------------------------------------------------------
    async def get_eq_bars_daily(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-daily: 株価四本値 (v2: /equities/bars/daily)

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 株価データ (v2のフィールド名で返却)
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        all_data = [
            item
            async for item in self._paginate(
                "/equities/bars/daily",
                params=params,
            )
        ]

        if not all_data:
            return pd.DataFrame(columns=constants.EQ_BARS_DAILY_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        sort_cols = [c for c in ["Code", "Date"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        return df.reset_index(drop=True)

    async def get_eq_bars_daily_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        全銘柄の株価四本値を日付範囲指定して取得 (v2: /equities/bars/daily)

        Args:
            start_dt: 取得開始日 (YYYYMMDD or YYYY-MM-DD)
            end_dt: 取得終了日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 株価データ (Code, Date 列でソート)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(*[self.get_eq_bars_daily(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates])
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # eq-bars-daily-am (/equities/bars/daily/am)
    # ------------------------------------------------------------------
    async def get_eq_bars_daily_am(self, code: str = "") -> pd.DataFrame:
        """
        eq-bars-daily-am: 前場四本値 (v2: /equities/bars/daily/am)

        Args:
            code: 銘柄コード (5桁 or 4桁)。空文字の場合は全銘柄。
        Returns:
            pd.DataFrame: 前場の株価データ
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code

        all_data = [item async for item in self._paginate("/equities/bars/daily/am", params=params)]

        if not all_data:
            return pd.DataFrame(columns=constants.PRICES_PRICES_AM_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Code" in df.columns:
            df.sort_values(["Code", "Date"], inplace=True)

        # v1 `/prices/prices_am` と同様に、前場四本値に対応する列のみを返す
        cols = constants.PRICES_PRICES_AM_COLUMNS_V2
        return df[cols].reset_index(drop=True)

    # ------------------------------------------------------------------
    # eq-bars-minute (/equities/bars/minute)
    # ------------------------------------------------------------------
    async def get_eq_bars_minute(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-minute: 分足 (v2: /equities/bars/minute)

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 1分足データ (v2のフィールド名で返却)
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        all_data = [
            item async for item in self._paginate("/equities/bars/minute", params=params, limiter=self._addon_limiter)
        ]

        cols = constants.EQ_BARS_MINUTE_COLUMNS_V2
        if not all_data:
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        sort_cols = [c for c in ["Code", "Date", "Time"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        return df[cols].reset_index(drop=True)

    def _aggregate_bars_n_minute(
        self,
        df: pd.DataFrame,
        n: int = 5,
    ) -> pd.DataFrame:
        return _aggregate_bars_n_minute(df, n)

    async def get_eq_bars_5minute(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-minute から5分足データを算出して取得

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 5分足データ
        """
        df_1min = await self.get_eq_bars_minute(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        return self._aggregate_bars_n_minute(df_1min, n=5)

    async def get_eq_bars_15minute(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-minute から15分足データを算出して取得

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 15分足データ
        """
        df_1min = await self.get_eq_bars_minute(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        return self._aggregate_bars_n_minute(df_1min, n=15)

    # ------------------------------------------------------------------
    # eq-investor-types (/equities/investor-types)
    # ------------------------------------------------------------------
    async def get_eq_investor_types(
        self,
        section: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-investor-types: 投資部門別売買状況 (v2: /equities/investor-types)

        Args:
            section: 市場区分 (例: \"TSEPrime\")
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
        Returns:
            pd.DataFrame: 投資部門別売買データ
        """
        params: dict[str, Any] = {}
        if section:
            params["section"] = section
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        all_data = [item async for item in self._paginate("/equities/investor-types", params=params)]

        if not all_data:
            return pd.DataFrame(columns=constants.EQ_INVESTOR_TYPES_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Section"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/markets/trades_spec` と同様に、定義済みカラムの順序で返す
        cols = constants.EQ_INVESTOR_TYPES_COLUMNS_V2
        return df[cols].reset_index(drop=True)

    # ------------------------------------------------------------------
    # /fins/summary (path_old: /fins/statements)
    # ------------------------------------------------------------------
    async def get_fin_summary(
        self,
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        財務情報サマリ (v2: /fins/summary)

        Args:
            code: 銘柄コード
            date_yyyymmdd: 開示日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 財務情報 (v2のフィールド名で返却)
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        all_data = [item async for item in self._paginate("/fins/summary", params=params, limiter=self._fins_limiter)]

        if not all_data:
            return pd.DataFrame(columns=constants.FIN_SUMMARY_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        for col in (
            "DiscDate",
            "CurPerSt",
            "CurPerEn",
            "CurFYSt",
            "CurFYEn",
            "NxtFYSt",
            "NxtFYEn",
        ):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "DiscTime", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/fins/statements` と同様に、定義済みカラムの順序で返す
        cols = constants.FIN_SUMMARY_COLUMNS_V2
        return df[cols].reset_index(drop=True)

    async def get_fin_summary_range(
        self,
        start_dt: DatetimeLike = "20080707",
        end_dt: DatetimeLike | None = None,
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務情報サマリを日付範囲指定して取得 (v2: /fins/summary)

        Args:
            start_dt: 取得開始日 (YYYYMMDD or YYYY-MM-DD)
            end_dt: 取得終了日 (YYYYMMDD or YYYY-MM-DD)
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ (未指定時はキャッシュしない)
        """
        _DATE_COLS = [
            "DiscDate",
            "CurPerSt",
            "CurPerEn",
            "CurFYSt",
            "CurFYEn",
            "NxtFYSt",
            "NxtFYEn",
        ]

        dates = pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D")
        cached_files: list[str] = []
        fetch_dates: list[str] = []

        for d in dates:
            yyyymmdd = d.strftime("%Y%m%d")
            cache_file = f"{cache_dir}/{yyyymmdd[:4]}/v2_fin_summary_{yyyymmdd}.csv.gz"
            if cache_dir and os.path.isfile(cache_file):
                cached_files.append(cache_file)
            else:
                fetch_dates.append(yyyymmdd)

        cache_dfs = await asyncio.gather(
            *[asyncio.to_thread(_read_fin_summary_cache, path, _DATE_COLS) for path in cached_files]
        )
        buff: list[pd.DataFrame] = list(cache_dfs)

        async def _fetch_and_cache(yyyymmdd: str) -> pd.DataFrame:
            df = await self.get_fin_summary(date_yyyymmdd=yyyymmdd)
            if cache_dir:
                cache_path = f"{cache_dir}/{yyyymmdd[:4]}/v2_fin_summary_{yyyymmdd}.csv.gz"
                await asyncio.to_thread(
                    _write_cache_atomic, cache_path, lambda p: df.to_csv(p, index=False, compression="gzip")
                )
            return df

        results = await asyncio.gather(*[_fetch_and_cache(d) for d in fetch_dates], return_exceptions=True)
        failures: list[BaseException] = []
        for result in results:
            if isinstance(result, BaseException):
                failures.append(result)
                continue
            if not result.empty:
                buff.append(result)

        if failures:
            # 各日のキャッシュは、その日の取得が成功した直後(gather 完了前)に既に
            # 書き込まれているので、呼び出し側が同じ cache_dir で再試行した際は
            # 失敗した日だけ再取得できる。
            raise failures[0]

        if not buff:
            return pd.DataFrame()

        return pd.concat(buff).sort_values(["DiscDate", "DiscTime", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /fins/details (path_old: /fins/fs_details)
    # ------------------------------------------------------------------
    async def get_fin_details(
        self,
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        財務諸表詳細 (v2: /fins/details)

        Args:
            code: 銘柄コード
            date_yyyymmdd: 開示日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 財務諸表詳細 (FS列に各項目が含まれる)
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        all_data = [item async for item in self._paginate("/fins/details", params=params, limiter=self._fins_limiter)]

        if not all_data:
            return pd.DataFrame(columns=constants.FINS_FS_DETAILS_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "DiscDate" in df.columns:
            df["DiscDate"] = pd.to_datetime(df["DiscDate"], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "DiscTime", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_fin_details_range(
        self,
        start_dt: DatetimeLike = "20080707",
        end_dt: DatetimeLike | None = None,
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務諸表詳細を日付範囲指定して取得 (v2: /fins/details)

        Args:
            start_dt: 取得開始日 (YYYYMMDD or YYYY-MM-DD)
            end_dt: 取得終了日 (YYYYMMDD or YYYY-MM-DD)
            cache_dir: Parquet形式のキャッシュファイルが存在するディレクトリ (未指定時はキャッシュしない)
        """
        dates = pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D")
        cached_files: list[str] = []
        fetch_dates: list[str] = []

        for d in dates:
            yyyymmdd = d.strftime("%Y%m%d")
            cache_file = f"{cache_dir}/{yyyymmdd[:4]}/v2_fin_details_{yyyymmdd}.parquet"
            if cache_dir and os.path.isfile(cache_file):
                cached_files.append(cache_file)
            else:
                fetch_dates.append(yyyymmdd)

        cache_dfs = await asyncio.gather(*[asyncio.to_thread(pd.read_parquet, path) for path in cached_files])
        buff: list[pd.DataFrame] = list(cache_dfs)

        async def _fetch_and_cache(yyyymmdd: str) -> pd.DataFrame:
            df = await self.get_fin_details(date_yyyymmdd=yyyymmdd)
            if cache_dir:
                cache_path = f"{cache_dir}/{yyyymmdd[:4]}/v2_fin_details_{yyyymmdd}.parquet"
                await asyncio.to_thread(_write_cache_atomic, cache_path, lambda p: df.to_parquet(p, index=False))
            return df

        results = await asyncio.gather(*[_fetch_and_cache(d) for d in fetch_dates], return_exceptions=True)
        failures: list[BaseException] = []
        for result in results:
            if isinstance(result, BaseException):
                failures.append(result)
                continue
            if not result.empty:
                buff.append(result)

        if failures:
            # 各日のキャッシュは、その日の取得が成功した直後(gather 完了前)に既に
            # 書き込まれているので、呼び出し側が同じ cache_dir で再試行した際は
            # 失敗した日だけ再取得できる。
            raise failures[0]

        if not buff:
            return pd.DataFrame()

        return pd.concat(buff).sort_values(["DiscDate", "DiscTime", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /fins/dividend (path_old: /fins/dividend)
    # ------------------------------------------------------------------
    async def get_fin_dividend(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        配当金情報 (v2: /fins/dividend)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 配当金データ
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        all_data = [item async for item in self._paginate("/fins/dividend", params=params)]

        if not all_data:
            return pd.DataFrame(columns=constants.FINS_DIVIDEND_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_fin_dividend_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        配当金情報を日付範囲指定して取得 (v2: /fins/dividend)

        Args:
            start_dt: 取得開始日 (YYYYMMDD or YYYY-MM-DD)
            end_dt: 取得終了日 (YYYYMMDD or YYYY-MM-DD)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(*[self.get_fin_dividend(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates])
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["PubDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /equities/earnings-calendar (path_old: /fins/announcement)
    # ------------------------------------------------------------------
    async def get_eq_earnings_cal(self) -> pd.DataFrame:
        """
        決算発表予定日 (v2: /equities/earnings-calendar)

        Returns:
            pd.DataFrame: 決算発表予定データ
        """

        all_data = [item async for item in self._paginate("/equities/earnings-calendar", params={})]

        if not all_data:
            return pd.DataFrame(columns=constants.FINS_ANNOUNCEMENT_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/short-ratio (path_old: /markets/short_selling)
    # ------------------------------------------------------------------
    async def get_mkt_short_ratio(
        self,
        sector_33_code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        業種別空売り比率 (v2: /markets/short-ratio)

        Args:
            sector_33_code: 33業種コード (例: 0050)
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 業種別空売り比率データ
        """
        params: dict[str, Any] = {}
        if sector_33_code:
            params["s33"] = sector_33_code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        all_data = [item async for item in self._paginate("/markets/short-ratio", params=params)]

        if not all_data:
            return pd.DataFrame(columns=constants.MKT_SHORT_RATIO_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "S33"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/markets/short_selling` と同様に、定義済みカラムの順序で返す
        cols = constants.MKT_SHORT_RATIO_COLUMNS_V2
        return df[cols].reset_index(drop=True)

    async def get_mkt_short_ratio_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        全33業種の空売り比率データを日付範囲指定して取得 (v2: /markets/short-ratio)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(*[self.get_mkt_short_ratio(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates])
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Date", "S33"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/short-sale-report (path_old: /markets/short_selling_positions)
    # ------------------------------------------------------------------
    async def get_mkt_short_sale_report(
        self,
        code: str = "",
        disclosed_date: str = "",
        disclosed_date_from: str = "",
        disclosed_date_to: str = "",
        calculated_date: str = "",
    ) -> pd.DataFrame:
        """
        空売り残高報告 (v2: /markets/short-sale-report)

        Args:
            code: 銘柄コード
            disclosed_date: 開示日
            disclosed_date_from: 開示日(開始)
            disclosed_date_to: 開示日(終了)
            calculated_date: 算出日
        Returns:
            pd.DataFrame: 空売り残高報告データ
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if disclosed_date:
            params["disc_date"] = disclosed_date
        if disclosed_date_from:
            params["disc_date_from"] = disclosed_date_from
        if disclosed_date_to:
            params["disc_date_to"] = disclosed_date_to
        if calculated_date:
            params["calc_date"] = calculated_date

        all_data = [item async for item in self._paginate("/markets/short-sale-report", params=params)]

        if not all_data:
            return pd.DataFrame(columns=constants.SHORT_SELLING_POSITIONS_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        for col in ("DiscDate", "CalcDate", "PrevRptDate"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "CalcDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_mkt_short_sale_report_range(
        self,
        start_dt: DatetimeLike = "20131107",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        空売り残高報告データを日付範囲指定して取得 (v2: /markets/short-sale-report)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_mkt_short_sale_report(disclosed_date=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["DiscDate", "CalcDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/margin-interest (path_old: /markets/weekly_margin_interest)
    # ------------------------------------------------------------------
    async def get_mkt_margin_interest(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        信用取引週末残高 (v2: /markets/margin-interest)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 信用取引週末残高データ
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = [item async for item in self._paginate("/markets/margin-interest", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.MARKETS_WEEKLY_MARGIN_INTEREST_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_mkt_margin_interest_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        信用取引週末残高を日付範囲指定して取得 (v2: /markets/margin-interest)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_mkt_margin_interest(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Date", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/margin-alert (path_old: /markets/daily_margin_interest)
    # ------------------------------------------------------------------
    async def get_mkt_margin_alert(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        日々公表信用取引残高 (v2: /markets/margin-alert)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 日々公表信用取引残高データ
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = [item async for item in self._paginate("/markets/margin-alert", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.DAILY_MARGIN_INTEREST_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_mkt_margin_alert_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        日々公表信用取引残高を日付範囲指定して取得 (v2: /markets/margin-alert)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_mkt_margin_alert(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["PubDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /edinet/major-shareholders
    # ------------------------------------------------------------------
    async def get_edinet_major_shareholders(
        self,
        edinet_code: str = "",
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        大株主状況 (v2: /edinet/major-shareholders)

        Args:
            edinet_code: EDINETコード (codeと同時指定不可)
            code: 銘柄コード (edinet_codeと同時指定不可)
            date_yyyymmdd: 提出日
        Returns:
            pd.DataFrame: 大株主状況データ (Hldrs列に大株主配列を含む)
        Raises:
            ValueError: edinet_code と code を同時に指定した場合
        """
        if edinet_code and code:
            raise ValueError("edinet_code と code は同時に指定できません")

        params: dict[str, Any] = {}
        if edinet_code:
            params["edinet_code"] = edinet_code
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        all_data = [
            item
            async for item in self._paginate("/edinet/major-shareholders", params=params, limiter=self._edinet_limiter)
        ]

        if not all_data:
            return pd.DataFrame(columns=constants.EDINET_MAJOR_SHAREHOLDERS_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "SubDate" in df.columns:
            df["SubDate"] = pd.to_datetime(df["SubDate"], errors="coerce")
        sort_cols = [c for c in ["SubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_edinet_major_shareholders_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        大株主状況を日付範囲指定して取得 (v2: /edinet/major-shareholders)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_edinet_major_shareholders(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["SubDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /edinet/cross-shareholdings
    # ------------------------------------------------------------------
    async def get_edinet_cross_shareholdings(
        self,
        edinet_code: str = "",
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        政策保有株式 (v2: /edinet/cross-shareholdings)

        Args:
            edinet_code: EDINETコード (codeと同時指定不可)
            code: 銘柄コード (edinet_codeと同時指定不可)
            date_yyyymmdd: 提出日
        Returns:
            pd.DataFrame: 政策保有株式データ
                (Report/Largest/SecondLargest列に保有主体ブロックを含む)
        Raises:
            ValueError: edinet_code と code を同時に指定した場合
        """
        if edinet_code and code:
            raise ValueError("edinet_code と code は同時に指定できません")

        params: dict[str, Any] = {}
        if edinet_code:
            params["edinet_code"] = edinet_code
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        all_data = [
            item
            async for item in self._paginate("/edinet/cross-shareholdings", params=params, limiter=self._edinet_limiter)
        ]

        if not all_data:
            return pd.DataFrame(columns=constants.EDINET_CROSS_SHAREHOLDINGS_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "SubDate" in df.columns:
            df["SubDate"] = pd.to_datetime(df["SubDate"], errors="coerce")
        sort_cols = [c for c in ["SubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_edinet_cross_shareholdings_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        政策保有株式を日付範囲指定して取得 (v2: /edinet/cross-shareholdings)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_edinet_cross_shareholdings(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["SubDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /edinet/large-volume-shareholders
    # ------------------------------------------------------------------
    async def get_edinet_large_volume_shareholders(
        self,
        edinet_code: str = "",
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        大量保有報告書 (v2: /edinet/large-volume-shareholders)

        Args:
            edinet_code: 発行者のEDINETコード (codeと同時指定不可)
            code: 発行者の銘柄コード (edinet_codeと同時指定不可)
            date_yyyymmdd: 提出日
        Returns:
            pd.DataFrame: 大量保有報告書データ (Hldrs列に保有者配列を含む)
        Raises:
            ValueError: edinet_code と code を同時に指定した場合
        """
        if edinet_code and code:
            raise ValueError("edinet_code と code は同時に指定できません")

        params: dict[str, Any] = {}
        if edinet_code:
            params["edinet_code"] = edinet_code
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        all_data = [
            item
            async for item in self._paginate(
                "/edinet/large-volume-shareholders", params=params, limiter=self._edinet_limiter
            )
        ]

        if not all_data:
            return pd.DataFrame(columns=constants.EDINET_LARGE_VOLUME_SHAREHOLDERS_COLUMNS_V2)

        df = pd.DataFrame.from_records(all_data)
        if "SubDate" in df.columns:
            df["SubDate"] = pd.to_datetime(df["SubDate"], errors="coerce")
        sort_cols = [c for c in ["SubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_edinet_large_volume_shareholders_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        大量保有報告書を日付範囲指定して取得 (v2: /edinet/large-volume-shareholders)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_edinet_large_volume_shareholders(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["SubDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/breakdown (path_old: /markets/breakdown)
    # ------------------------------------------------------------------
    async def get_mkt_breakdown(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        売買内訳データ (v2: /markets/breakdown)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 売買内訳データ
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = [item async for item in self._paginate("/markets/breakdown", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.MKT_BREAKDOWN_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Code", "Date"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/markets/breakdown` と同様に、定義済みカラムの順序で返す
        cols = constants.MKT_BREAKDOWN_COLUMNS_V2
        return df[cols].reset_index(drop=True)

    async def get_mkt_breakdown_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        売買内訳データを日付範囲指定して取得 (v2: /markets/breakdown)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(*[self.get_mkt_breakdown(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates])
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/calendar (path_old: /markets/trading_calendar)
    # ------------------------------------------------------------------
    async def get_mkt_calendar(
        self,
        holiday_division: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        取引カレンダー (v2: /markets/calendar)

        Args:
            holiday_division: 休日区分 (HolDiv コード)
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
        Returns:
            pd.DataFrame: 取引カレンダーデータ
        """
        params: dict[str, Any] = {}
        if holiday_division:
            params["hol_div"] = holiday_division
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        data = [item async for item in self._paginate("/markets/calendar", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.MARKETS_TRADING_CALENDAR_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.sort_values("Date", inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # indices (v2: /indices/bars/daily, /indices/bars/daily/topix)
    # ------------------------------------------------------------------
    async def get_idx_bars_daily(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        指数四本値 (v2: /indices/bars/daily)

        Args:
            code: 指数コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        # v1 と同様: date があれば date 優先、なければ from/to
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = [item async for item in self._paginate("/indices/bars/daily", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.INDICES_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            sort_cols: list[str] = []
            if "Code" in df.columns:
                sort_cols.append("Code")
            sort_cols.append("Date")
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_idx_bars_daily_topix(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        TOPIX 指数四本値 (v2: /indices/bars/daily/topix)

        Args:
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
        """
        params: dict[str, Any] = {}
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        data = [item async for item in self._paginate("/indices/bars/daily/topix", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.INDICES_TOPIX_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.sort_values("Date", inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # derivatives (v2: /derivatives/bars/daily/*)
    # ------------------------------------------------------------------
    async def get_drv_bars_daily_fut(
        self,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        """
        先物四本値 (v2: /derivatives/bars/daily/futures)
        """
        params: dict[str, Any] = {"date": date_yyyymmdd}
        if category:
            params["category"] = category
        if contract_flag:
            params["contract_flag"] = contract_flag

        data = [item async for item in self._paginate("/derivatives/bars/daily/futures", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.DERIVATIVES_FUTURES_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols: list[str] = []
        if "Code" in df.columns:
            sort_cols.append("Code")
        if "Date" in df.columns:
            sort_cols.append("Date")
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_drv_bars_daily_opt(
        self,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        """
        オプション四本値 (v2: /derivatives/bars/daily/options)
        """
        params: dict[str, Any] = {"date": date_yyyymmdd}
        if category:
            params["category"] = category
        if contract_flag:
            params["contract_flag"] = contract_flag
        if code:
            params["code"] = code

        data = [item async for item in self._paginate("/derivatives/bars/daily/options", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.DERIVATIVES_OPTIONS_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols: list[str] = []
        if "Code" in df.columns:
            sort_cols.append("Code")
        if "Date" in df.columns:
            sort_cols.append("Date")
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_drv_bars_daily_opt_225(
        self,
        date_yyyymmdd: str,
    ) -> pd.DataFrame:
        """
        日経225オプション四本値 (v2: /derivatives/bars/daily/options/225)
        """
        params: dict[str, Any] = {"date": date_yyyymmdd}

        data = [item async for item in self._paginate("/derivatives/bars/daily/options/225", params=params)]
        if not data:
            return pd.DataFrame(columns=constants.DERIVATIVES_OPTIONS_COLUMNS_V2)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols: list[str] = []
        if "Code" in df.columns:
            sort_cols.append("Code")
        if "Date" in df.columns:
            sort_cols.append("Date")
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    async def get_drv_bars_daily_fut_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        """
        先物四本値を日付範囲指定して取得 (v2: /derivatives/bars/daily/futures)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[
                self.get_drv_bars_daily_fut(
                    date_yyyymmdd=d.strftime("%Y-%m-%d"),
                    category=category,
                    contract_flag=contract_flag,
                )
                for d in dates
            ]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    async def get_drv_bars_daily_opt_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        """
        オプション四本値を日付範囲指定して取得 (v2: /derivatives/bars/daily/options)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[
                self.get_drv_bars_daily_opt(
                    date_yyyymmdd=d.strftime("%Y-%m-%d"),
                    category=category,
                    contract_flag=contract_flag,
                    code=code,
                )
                for d in dates
            ]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    async def get_drv_bars_daily_opt_225_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike | None = None,
    ) -> pd.DataFrame:
        """
        日経225オプション四本値を日付範囲指定して取得 (v2: /derivatives/bars/daily/options/225)
        """
        dates = list(pd.date_range(start_dt, end_dt or datetime.now().strftime("%Y%m%d"), freq="D"))
        buff: list[pd.DataFrame] = []
        results = await asyncio.gather(
            *[self.get_drv_bars_daily_opt_225(date_yyyymmdd=d.strftime("%Y-%m-%d")) for d in dates]
        )
        buff.extend(df for df in results if not df.empty)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # bulk (/bulk/list, /bulk/get)
    # ------------------------------------------------------------------

    async def get_bulk_list(self, endpoint: BulkEndpoint | str) -> pd.DataFrame:
        """
        バルクデータの一覧取得 (v2: /bulk/list)

        Args:
            endpoint: 対象エンドポイント (BulkEndpoint enum または文字列, 例: "/equities/master")
        Returns:
            pd.DataFrame: Key, Size, LastModified 列を持つデータフレーム
        """
        endpoint_value = endpoint.value if isinstance(endpoint, BulkEndpoint) else endpoint
        params: dict[str, Any] = {"endpoint": endpoint_value}
        body = (await self._get("/bulk/list", params)).json()
        data = body.get("data", [])
        if not data:
            return pd.DataFrame(columns=constants.BULK_LIST_COLUMNS_V2)
        df = pd.DataFrame.from_records(data)
        if "LastModified" in df.columns:
            df["LastModified"] = pd.to_datetime(df["LastModified"], errors="coerce")
        return df[constants.BULK_LIST_COLUMNS_V2].reset_index(drop=True)

    async def get_bulk(self, key: str) -> str:
        """
        バルクデータのダウンロード URL 取得 (v2: /bulk/get)

        Args:
            key: get_bulk_list() の Key 列の値
        Returns:
            str: ダウンロード URL
        """
        body = (await self._get("/bulk/get", {"key": key})).json()
        return body["url"]

    async def download_bulk(self, key: str, output_path: str) -> None:
        """
        バルクデータをファイルにダウンロード

        Args:
            key: get_bulk_list() の Key 列の値
            output_path: 保存先ファイルパス
        """
        url = await self.get_bulk(key)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient(timeout=300.0) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        f.write(chunk)
