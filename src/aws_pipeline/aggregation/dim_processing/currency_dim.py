from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from ...schemas.gold_schema import CurrencyDimensionSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


DEFAULT_CURRENCY_CODE = "USD"

# ISO 4217 numeric currency codes fit in ShortType and remain stable across
# independent builds of this static dimension.
ISO_4217_NUMERIC_CODES = {
    "AED": 784,
    "AFN": 971,
    "ALL": 8,
    "AMD": 51,
    "ANG": 532,
    "AOA": 973,
    "ARS": 32,
    "AUD": 36,
    "AWG": 533,
    "AZN": 944,
    "BAM": 977,
    "BBD": 52,
    "BDT": 50,
    "BGN": 975,
    "BHD": 48,
    "BIF": 108,
    "BMD": 60,
    "BND": 96,
    "BOB": 68,
    "BOV": 984,
    "BRL": 986,
    "BSD": 44,
    "BTN": 64,
    "BWP": 72,
    "BYN": 933,
    "BZD": 84,
    "CAD": 124,
    "CDF": 976,
    "CHE": 947,
    "CHF": 756,
    "CHW": 948,
    "CLF": 990,
    "CLP": 152,
    "CNY": 156,
    "COP": 170,
    "COU": 970,
    "CRC": 188,
    "CUP": 192,
    "CVE": 132,
    "CZK": 203,
    "DJF": 262,
    "DKK": 208,
    "DOP": 214,
    "DZD": 12,
    "EGP": 818,
    "ERN": 232,
    "ETB": 230,
    "EUR": 978,
    "FJD": 242,
    "FKP": 238,
    "GBP": 826,
    "GEL": 981,
    "GHS": 936,
    "GIP": 292,
    "GMD": 270,
    "GNF": 324,
    "GTQ": 320,
    "GYD": 328,
    "HKD": 344,
    "HNL": 340,
    "HTG": 332,
    "HUF": 348,
    "IDR": 360,
    "ILS": 376,
    "INR": 356,
    "IQD": 368,
    "IRR": 364,
    "ISK": 352,
    "JMD": 388,
    "JOD": 400,
    "JPY": 392,
    "KES": 404,
    "KGS": 417,
    "KHR": 116,
    "KMF": 174,
    "KPW": 408,
    "KRW": 410,
    "KWD": 414,
    "KYD": 136,
    "KZT": 398,
    "LAK": 418,
    "LBP": 422,
    "LKR": 144,
    "LRD": 430,
    "LSL": 426,
    "LYD": 434,
    "MAD": 504,
    "MDL": 498,
    "MGA": 969,
    "MKD": 807,
    "MMK": 104,
    "MNT": 496,
    "MOP": 446,
    "MRU": 929,
    "MUR": 480,
    "MVR": 462,
    "MWK": 454,
    "MXN": 484,
    "MXV": 979,
    "MYR": 458,
    "MZN": 943,
    "NAD": 516,
    "NGN": 566,
    "NIO": 558,
    "NOK": 578,
    "NPR": 524,
    "NZD": 554,
    "OMR": 512,
    "PAB": 590,
    "PEN": 604,
    "PGK": 598,
    "PHP": 608,
    "PKR": 586,
    "PLN": 985,
    "PYG": 600,
    "QAR": 634,
    "RON": 946,
    "RSD": 941,
    "RUB": 643,
    "RWF": 646,
    "SAR": 682,
    "SBD": 90,
    "SCR": 690,
    "SDG": 938,
    "SEK": 752,
    "SGD": 702,
    "SHP": 654,
    "SLE": 925,
    "SLL": 694,
    "SOS": 706,
    "SRD": 968,
    "SSP": 728,
    "STN": 930,
    "SVC": 222,
    "SYP": 760,
    "SZL": 748,
    "THB": 764,
    "TJS": 972,
    "TMT": 934,
    "TND": 788,
    "TOP": 776,
    "TRY": 949,
    "TTD": 780,
    "TWD": 901,
    "TZS": 834,
    "UAH": 980,
    "UGX": 800,
    "USD": 840,
    "USN": 997,
    "UYI": 940,
    "UYU": 858,
    "UYW": 927,
    "UZS": 860,
    "VED": 926,
    "VES": 928,
    "VND": 704,
    "VUV": 548,
    "WST": 882,
    "XAF": 950,
    "XAG": 961,
    "XAU": 959,
    "XBA": 955,
    "XBB": 956,
    "XBC": 957,
    "XBD": 958,
    "XCD": 951,
    "XDR": 960,
    "XOF": 952,
    "XPD": 964,
    "XPF": 953,
    "XPT": 962,
    "XSU": 994,
    "XTS": 963,
    "XUA": 965,
    "XXX": 999,
    "YER": 886,
    "ZAR": 710,
    "ZMW": 967,
    "ZWG": 924,
}


def _normalize_currency_codes(currencies: Optional[Sequence[str]]) -> list[str]:
    if not currencies:
        return [DEFAULT_CURRENCY_CODE]

    currency_codes = sorted(
        {
            str(currency).strip().upper()
            for currency in currencies
            if currency is not None and str(currency).strip()
        }
    )

    return currency_codes or [DEFAULT_CURRENCY_CODE]


def generate_currency_dim(
    spark: SparkSession,
    batch_logical_date,
    currencies: Optional[Sequence[str]] = None,
) -> DataFrame:
    """
    Generate the static Currency dimension from provided ISO 4217 currency codes.
    """
    currency_codes = _normalize_currency_codes(currencies)
    unsupported_codes = [
        currency_code
        for currency_code in currency_codes
        if currency_code not in ISO_4217_NUMERIC_CODES
    ]
    if unsupported_codes:
        raise ValueError(
            "Unsupported ISO 4217 currency code(s): "
            + ", ".join(sorted(unsupported_codes))
        )

    currency_rows = [
        (ISO_4217_NUMERIC_CODES[currency_code], currency_code)
        for currency_code in currency_codes
    ]
    currency_dim_df = spark.createDataFrame(
        currency_rows,
        ["currency_key", "currency_type"],
    ).select(
        F.col("currency_key").cast("short").alias("currency_key"),
        F.col("currency_type"),
    )

    currency_dim_df = add_audit_columns(currency_dim_df, batch_logical_date)
    currency_dim_df = schema_enforcing(currency_dim_df, CurrencyDimensionSchema)

    return currency_dim_df
