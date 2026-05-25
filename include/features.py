"""
CryptoPulse - Feature Engineering Helper Module
================================================
Ye module raw OHLCV data se trading indicators compute karta hai:
  - RSI (Relative Strength Index) - 14 period
  - MACD & Signal Line
  - Moving Averages (Short=7, Long=25)
  - Target Label (1=Price Up, 0=Price Down next candle)
"""

import pandas as pd
import numpy as np


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    RSI (Relative Strength Index) calculate karta hai.
    0-30: Oversold (buy signal), 70-100: Overbought (sell signal)
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.round(4)


def compute_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    MACD (Moving Average Convergence Divergence) calculate karta hai.
    Returns: (macd_line, signal_line)
    """
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = (ema_fast - ema_slow).round(4)
    signal_line = macd_line.ewm(span=signal, adjust=False).mean().round(4)
    return macd_line, signal_line


def compute_moving_averages(series: pd.Series, short: int = 7, long_: int = 25):
    """
    Short-term aur Long-term Moving Averages calculate karta hai.
    Returns: (ma_short, ma_long)
    """
    ma_short = series.rolling(window=short, min_periods=1).mean().round(8)
    ma_long = series.rolling(window=long_, min_periods=1).mean().round(8)
    return ma_short, ma_long


def compute_target_label(close_series: pd.Series) -> pd.Series:
    """
    Next candle ka price agar badhega toh 1, nahi toh 0.
    Supervised learning ke liye binary classification target.
    """
    return (close_series.shift(-1) > close_series).astype(int)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full feature engineering pipeline.
    Input: DataFrame with columns [timestamp, close_price, open_price, high_price, low_price, volume]
    Output: DataFrame with all computed features
    """
    df = df.copy().sort_values("timestamp").reset_index(drop=True)
    close = df["close_price"]

    df["rsi_14"] = compute_rsi(close, period=14)
    df["macd"], df["macd_signal"] = compute_macd(close)
    df["ma_short"], df["ma_long"] = compute_moving_averages(close)
    df["target_label"] = compute_target_label(close)

    # Pehle 26 rows mein NaN ho sakta hai MACD ke liye - drop karo
    df = df.dropna(subset=["rsi_14", "macd", "macd_signal"])

    return df[["timestamp", "close_price", "rsi_14", "macd", "macd_signal", "ma_short", "ma_long", "target_label"]]
