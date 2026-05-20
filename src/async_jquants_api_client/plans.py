from enum import Enum


class Plan(Enum):
    FREE = "free"
    LIGHT = "light"
    STANDARD = "standard"
    PREMIUM = "premium"


class BulkEndpoint(Enum):
    EQ_MASTER = "/equities/master"
    EQ_BARS_DAILY = "/equities/bars/daily"
    EQ_BARS_MINUTE = "/equities/bars/minute"
    EQ_INVESTOR_TYPES = "/equities/investor-types"
    EQ_TRADES = "/equities/trades"
    FIN_SUMMARY = "/fins/summary"
    FIN_DETAILS = "/fins/details"
    FIN_DIVIDEND = "/fins/dividend"
    MKT_SHORT_RATIO = "/markets/short-ratio"
    MKT_SHORT_SALE_REPORT = "/markets/short-sale-report"
    MKT_MARGIN_INTEREST = "/markets/margin-interest"
    MKT_MARGIN_ALERT = "/markets/margin-alert"
    MKT_BREAKDOWN = "/markets/breakdown"
    IDX_BARS_DAILY = "/indices/bars/daily"
    IDX_BARS_DAILY_TOPIX = "/indices/bars/daily/topix"
    DRV_BARS_DAILY_FUT = "/derivatives/bars/daily/futures"
    DRV_BARS_DAILY_OPT = "/derivatives/bars/daily/options"
    DRV_BARS_DAILY_OPT_225 = "/derivatives/bars/daily/options/225"
