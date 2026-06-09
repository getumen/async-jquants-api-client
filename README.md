# async-jquants-api-client

Async Python client for [JQuants API v2](https://jpx-jquants.com/).

[jquants-api-client-python](https://github.com/J-Quants/jquants-api-client-python) の以下の点を改善したライブラリです：

- **スレッドセーフでない・`requests.Session` ベース** → `httpx` + `asyncio` による完全非同期実装
- **v2 のレートリミット未対応** → `aiolimiter` によるプラン別レートリミット対応

## 要件

- Python 3.10+
- JQuants API キー（[JQuants](https://jpx-jquants.com/) で取得）

## インストール

### GitHubからのインストール

アルファ版のため、バージョン管理されていません

pip install git+https://github.com/getumen/async-jquants-api-client.git

### TBD: PyPIによるインストール

アルファ版のためPyPIにリリースされていません

```bash
pip install async-jquants-api-client
```

## 設定

API キーは以下の優先順位で読み込まれます：

1. コード内で直接指定
2. 環境変数 `JQUANTS_API_KEY`
3. 設定ファイル `jquants-api.toml`（カレントディレクトリ → `~/.jquants-api/` の順）

### 設定ファイルの例

```toml
[jquants-api-client]
api_key = "your_api_key_here"
```

## 基本的な使い方

```python
import asyncio
from async_jquants_api_client import JQuantsClientV2, Plan

async def main():
    async with JQuantsClientV2(api_key="your_api_key", plan=Plan.STANDARD) as client:
        # 株価日足（1日分）
        df = await client.get_eq_bars_daily(code="86970", date_yyyymmdd="2024-01-05")
        print(df)

        # 株価日足（期間指定、全銘柄を並列取得）
        df = await client.get_eq_bars_daily_range("20240101", "20240131")
        print(df)

asyncio.run(main())
```

## プラン別レートリミット

| プラン | リクエスト数/分 |
|--------|---------------|
| FREE | 5 |
| LIGHT | 60 |
| STANDARD | 120 |
| PREMIUM | 500 |
| fins/summary (全プラン共通) | 60 |
| fins/details (全プラン共通) | 60 |

```python
from async_jquants_api_client import JQuantsClientV2, Plan

async with JQuantsClientV2(api_key="your_api_key", plan=Plan.PREMIUM) as client:
    ...
```

## API リファレンス

### 株式マスター

```python
# 上場銘柄一覧
df = await client.get_eq_master(code="86970", date="20240105")

# 上場銘柄一覧（業種・市場区分の英語名付き）
df = await client.get_list()
```

### 株価

```python
# 日足
df = await client.get_eq_bars_daily(code="86970", date_yyyymmdd="2024-01-05")
df = await client.get_eq_bars_daily(code="86970", from_yyyymmdd="20240101", to_yyyymmdd="20240131")

# 日足（日付範囲、全銘柄を並列取得）
df = await client.get_eq_bars_daily_range("20240101", "20240131")

# 前場四本値
df = await client.get_eq_bars_daily_am(code="86970")

# 分足・5分足・15分足
df = await client.get_eq_bars_minute(code="86970", date_yyyymmdd="2024-01-05")
df = await client.get_eq_bars_5minute(code="86970", date_yyyymmdd="2024-01-05")
df = await client.get_eq_bars_15minute(code="86970", date_yyyymmdd="2024-01-05")
```

### 財務情報

```python
# 財務情報サマリ
df = await client.get_fin_summary(code="86970")
df = await client.get_fin_summary_range("20240101", "20240131", cache_dir="/path/to/cache")

# 財務諸表詳細
df = await client.get_fin_details(date_yyyymmdd="2024-01-05")
df = await client.get_fin_details_range("20240101", "20240131", cache_dir="/path/to/cache")

# 配当金情報
df = await client.get_fin_dividend(code="86970")

# 決算発表予定
df = await client.get_eq_earnings_cal()
```

### 市場統計

```python
# 空売り比率
df = await client.get_mkt_short_ratio(date_yyyymmdd="2024-01-05")
df = await client.get_mkt_short_ratio_range("20240101", "20240131")

# 空売り残高報告
df = await client.get_mkt_short_sale_report(disclosed_date="2024-01-05")
df = await client.get_mkt_short_sale_report_range("20240101", "20240131")

# 信用取引週末残高
df = await client.get_mkt_margin_interest(date_yyyymmdd="2024-01-05")
df = await client.get_mkt_margin_interest_range("20240101", "20240131")

# 日々公表信用取引残高
df = await client.get_mkt_margin_alert(date_yyyymmdd="2024-01-05")
df = await client.get_mkt_margin_alert_range("20240101", "20240131")

# 売買内訳
df = await client.get_mkt_breakdown(date_yyyymmdd="2024-01-05")
df = await client.get_mkt_breakdown_range("20240101", "20240131")

# 取引カレンダー
df = await client.get_mkt_calendar(from_yyyymmdd="20240101", to_yyyymmdd="20240131")

# 投資部門別売買状況
df = await client.get_eq_investor_types(section="TSEPrime")
```

### 指数

```python
# 指数四本値
df = await client.get_idx_bars_daily(code="0000", date_yyyymmdd="2024-01-05")

# TOPIX
df = await client.get_idx_bars_daily_topix(from_yyyymmdd="20240101", to_yyyymmdd="20240131")
```

### デリバティブ

```python
# 先物四本値
df = await client.get_drv_bars_daily_fut(date_yyyymmdd="2024-01-05")
df = await client.get_drv_bars_daily_fut_range("20240101", "20240131")

# オプション四本値
df = await client.get_drv_bars_daily_opt(date_yyyymmdd="2024-01-05")
df = await client.get_drv_bars_daily_opt_range("20240101", "20240131")

# 日経225オプション四本値
df = await client.get_drv_bars_daily_opt_225(date_yyyymmdd="2024-01-05")
df = await client.get_drv_bars_daily_opt_225_range("20240101", "20240131")
```

### マスターデータ（ローカル）

```python
# 17業種・33業種・市場区分（APIリクエストなし）
df = client.get_17_sectors()
df = client.get_33_sectors()
df = client.get_market_segments()
```

## キャッシュ

`get_fin_summary_range` と `get_fin_details_range` は `cache_dir` を指定することで日付ごとに CSV.gz ファイルとしてキャッシュできます。

```python
df = await client.get_fin_summary_range(
    "20200101", "20240131",
    cache_dir="/path/to/cache"
)
```

キャッシュファイルは `{cache_dir}/{yyyy}/v2_fin_summary_{yyyymmdd}.csv.gz` の形式で保存されます。

## エラーハンドリング

```python
from async_jquants_api_client import JQuantsAuthError, JQuantsAPIError

try:
    df = await client.get_eq_bars_daily(code="86970", date_yyyymmdd="2024-01-05")
except JQuantsAuthError:
    # 401/403: APIキーが無効
    ...
except JQuantsAPIError as e:
    # その他のAPIエラー（リトライ済み）
    print(e.status_code, e.message)
```

429/500/502/503/504 は指数バックオフで最大3回自動リトライします。

## ライセンス

Apache License 2.0
