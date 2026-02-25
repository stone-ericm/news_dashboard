"""Seasonality detection and baseline computation."""

from typing import Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.tsa.seasonal import STL


class SeasonalityProcessor:
    """Process time series to extract seasonal baselines and compute z-scores."""
    
    def __init__(
        self,
        seasonal_period: int = 7,
        trend_window: Optional[int] = None,
        seasonal_window: Optional[int] = None,
        robust: bool = True,
    ):
        """
        Initialize seasonality processor.
        
        Args:
            seasonal_period: Period of seasonality (7 for weekly, 365 for annual)
            trend_window: Window for trend extraction (odd number)
            seasonal_window: Window for seasonal extraction (odd number)
            robust: Use robust STL (resistant to outliers)
        """
        self.seasonal_period = seasonal_period
        self.trend_window = trend_window
        self.seasonal_window = seasonal_window
        self.robust = robust
    
    def decompose(self, series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Decompose time series into trend, seasonal, and residual components.
        
        Args:
            series: Time series data (must have datetime index)
        
        Returns:
            Tuple of (trend, seasonal, residual) series
        """
        if len(series) < 2 * self.seasonal_period:
            # Not enough data for decomposition
            return series, pd.Series(0, index=series.index), pd.Series(0, index=series.index)
        
        # Ensure numeric type
        series = pd.to_numeric(series, errors="coerce")
        
        # Handle missing values
        series = series.ffill().bfill()
        
        # Perform STL decomposition
        # Ensure seasonal window is odd
        seasonal_window = self.seasonal_window
        if seasonal_window is None:
            seasonal_window = 7  # Default odd window
        elif seasonal_window % 2 == 0:
            seasonal_window += 1
            
        stl = STL(
            series,
            period=self.seasonal_period,
            trend=self.trend_window,
            seasonal=seasonal_window,
            robust=self.robust,
        )
        result = stl.fit()
        
        return result.trend, result.seasonal, result.resid
    
    def compute_baseline(
        self,
        series: pd.Series,
        window_days: int = 90,
    ) -> pd.Series:
        """
        Compute seasonal baseline for a time series.
        
        Args:
            series: Time series data
            window_days: Rolling window for baseline calculation
        
        Returns:
            Baseline series
        """
        # Decompose
        trend, seasonal, _ = self.decompose(series)
        
        # Baseline is trend + seasonal
        baseline = trend + seasonal
        
        # Apply additional smoothing if needed
        if window_days > 0:
            baseline = baseline.rolling(
                window=window_days,
                min_periods=self.seasonal_period,
                center=True,
            ).mean()
            baseline = baseline.ffill().bfill()
        
        return baseline
    
    def compute_zscore(
        self,
        series: pd.Series,
        baseline: Optional[pd.Series] = None,
        window_days: int = 90,
        min_std: float = 0.01,
    ) -> pd.Series:
        """
        Compute z-score relative to seasonal baseline.
        
        Args:
            series: Time series data
            baseline: Pre-computed baseline (optional)
            window_days: Window for std calculation
            min_std: Minimum standard deviation (to avoid division by near-zero)
        
        Returns:
            Z-score series
        """
        if baseline is None:
            baseline = self.compute_baseline(series, window_days)
        
        # Compute residuals
        residuals = series - baseline
        
        # Compute rolling standard deviation
        rolling_std = residuals.rolling(
            window=window_days,
            min_periods=self.seasonal_period,
            center=True,
        ).std()
        
        # Fill missing values
        rolling_std = rolling_std.ffill().bfill()
        
        # Apply minimum std threshold
        rolling_std = rolling_std.clip(lower=min_std)
        
        # Compute z-scores
        z_scores = residuals / rolling_std
        
        return z_scores
    
    def detect_anomalies(
        self,
        series: pd.Series,
        z_threshold: float = 3.0,
        min_consecutive: int = 1,
    ) -> pd.Series:
        """
        Detect anomalies based on z-scores.
        
        Args:
            series: Time series data
            z_threshold: Z-score threshold for anomalies
            min_consecutive: Minimum consecutive anomalies to flag
        
        Returns:
            Boolean series indicating anomalies
        """
        z_scores = self.compute_zscore(series)
        
        # Flag points exceeding threshold
        anomalies = np.abs(z_scores) > z_threshold
        
        if min_consecutive > 1:
            # Require consecutive anomalies
            anomalies = anomalies.rolling(
                window=min_consecutive,
                min_periods=min_consecutive,
            ).sum() >= min_consecutive
        
        return anomalies
    
    def compute_trend_slope(
        self,
        series: pd.Series,
        window_days: int = 7,
    ) -> pd.Series:
        """
        Compute trend slope over a rolling window.
        
        Args:
            series: Time series data
            window_days: Window for slope calculation
        
        Returns:
            Slope series (change per day)
        """
        def _rolling_slope(x):
            """Calculate slope using linear regression."""
            if len(x) < 2:
                return np.nan
            
            # Create time index
            t = np.arange(len(x))
            
            # Fit linear regression
            try:
                slope, _ = np.polyfit(t, x, 1)
                return slope
            except Exception:
                return np.nan
        
        slopes = series.rolling(
            window=window_days,
            min_periods=2,
        ).apply(_rolling_slope, raw=True)
        
        return slopes
    
    def compute_volatility(
        self,
        series: pd.Series,
        window_days: int = 30,
    ) -> pd.Series:
        """
        Compute rolling volatility (coefficient of variation).
        
        Args:
            series: Time series data
            window_days: Window for volatility calculation
        
        Returns:
            Volatility series
        """
        rolling_mean = series.rolling(
            window=window_days,
            min_periods=self.seasonal_period,
        ).mean()
        
        rolling_std = series.rolling(
            window=window_days,
            min_periods=self.seasonal_period,
        ).std()
        
        # Coefficient of variation
        volatility = rolling_std / rolling_mean.abs().clip(lower=0.01)
        
        return volatility


class MultiSignalProcessor:
    """Process multiple signals for a topic."""
    
    def __init__(self, seasonality_processor: Optional[SeasonalityProcessor] = None):
        """
        Initialize multi-signal processor.
        
        Args:
            seasonality_processor: Seasonality processor instance
        """
        self.seasonality_processor = seasonality_processor or SeasonalityProcessor()
    
    def aggregate_signals(
        self,
        signals: pd.DataFrame,
        weights: Optional[dict] = None,
    ) -> pd.Series:
        """
        Aggregate multiple signals into a single score.
        
        Args:
            signals: DataFrame with signal columns
            weights: Optional weights for each signal
        
        Returns:
            Aggregated score series
        """
        if weights is None:
            # Equal weights
            weights = {col: 1.0 for col in signals.columns}
        
        # Normalize weights
        total_weight = sum(weights.values())
        weights = {k: v / total_weight for k, v in weights.items()}
        
        # Compute weighted average
        aggregated = pd.Series(0, index=signals.index)
        for col in signals.columns:
            if col in weights:
                aggregated += signals[col] * weights[col]
        
        return aggregated
    
    def compute_features(self, series: pd.Series) -> pd.DataFrame:
        """
        Compute all features for a time series.
        
        Args:
            series: Time series data
        
        Returns:
            DataFrame with computed features
        """
        features = pd.DataFrame(index=series.index)
        
        # Raw value
        features["value"] = series
        
        # Baseline and z-score
        baseline = self.seasonality_processor.compute_baseline(series)
        features["baseline"] = baseline
        features["z_score"] = self.seasonality_processor.compute_zscore(series, baseline)
        
        # Trend slope
        features["slope_7d"] = self.seasonality_processor.compute_trend_slope(series, 7)
        features["slope_30d"] = self.seasonality_processor.compute_trend_slope(series, 30)
        
        # Volatility
        features["volatility_30d"] = self.seasonality_processor.compute_volatility(series, 30)
        
        # Anomalies
        features["is_anomaly"] = self.seasonality_processor.detect_anomalies(series)
        
        # Percent change
        features["pct_change_7d"] = series.pct_change(periods=7)
        features["pct_change_30d"] = series.pct_change(periods=30)
        
        return features
