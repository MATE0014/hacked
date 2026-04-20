"""
Data analysis module for statistical analysis and structure detection
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from scipy import stats


class Analyzer:
    """Performs comprehensive statistical and structural analysis"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.numeric_cols = self._get_numeric_columns()
        self.datetime_cols = self._get_datetime_columns()
        self.categorical_cols = self._get_categorical_columns()
    
    def _get_numeric_columns(self) -> List[str]:
        """Get all numeric columns"""
        return self.df.select_dtypes(include=[np.number]).columns.tolist()
    
    def _get_categorical_columns(self) -> List[str]:
        """Get all categorical/object columns (excluding datetime)"""
        obj_cols = self.df.select_dtypes(include=['object']).columns.tolist()
        # Filter out any datetime-like columns
        return [col for col in obj_cols if col not in self.datetime_cols]
    
    def _get_datetime_columns(self) -> List[str]:
        """Get all datetime columns"""
        # Include both naive and timezone-aware datetime dtypes.
        return self.df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Generate comprehensive statistics"""
        stats_report = {
            "overview": {
                "total_rows": len(self.df),
                "total_columns": len(self.df.columns),
                "memory_usage_mb": self.df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            "numeric_analysis": self._numeric_statistics(),
            "categorical_analysis": self._categorical_statistics(),
            "correlations": self._calculate_correlations(),
            "anomalies": self._detect_anomalies()
        }
        
        return stats_report
    
    def _numeric_statistics(self) -> Dict[str, Any]:
        """Calculate statistics for numeric columns"""
        if not self.numeric_cols:
            return {}
        
        stats_data = {}
        for col in self.numeric_cols:
            col_data = self.df[col].dropna()
            
            stats_data[col] = {
                "mean": float(col_data.mean()),
                "median": float(col_data.median()),
                "std": float(col_data.std()),
                "min": float(col_data.min()),
                "max": float(col_data.max()),
                "q1": float(col_data.quantile(0.25)),
                "q3": float(col_data.quantile(0.75)),
                "skewness": float(stats.skew(col_data)),
                "kurtosis": float(stats.kurtosis(col_data)),
                "null_count": int(self.df[col].isna().sum())
            }
        
        return stats_data
    
    def _categorical_statistics(self) -> Dict[str, Any]:
        """Calculate statistics for categorical columns"""
        if not self.categorical_cols:
            return {}
        
        stats_data = {}
        for col in self.categorical_cols:
            value_counts = self.df[col].value_counts().head(10)
            
            stats_data[col] = {
                "unique_values": int(self.df[col].nunique()),
                "top_values": value_counts.to_dict(),
                "null_count": int(self.df[col].isna().sum()),
                "mode": str(self.df[col].mode()[0]) if len(self.df[col].mode()) > 0 else None
            }
        
        return stats_data
    
    def _calculate_correlations(self) -> Dict[str, Any]:
        """Calculate correlation matrix for numeric columns"""
        if len(self.numeric_cols) < 2:
            return {}
        
        correlation_matrix = self.df[self.numeric_cols].corr()
        
        # Find strongest correlations
        correlations = {}
        for i in range(len(correlation_matrix.columns)):
            for j in range(i + 1, len(correlation_matrix.columns)):
                corr_val = correlation_matrix.iloc[i, j]
                if abs(corr_val) > 0.3:  # Only include moderate+ correlations
                    key = f"{correlation_matrix.columns[i]} <-> {correlation_matrix.columns[j]}"
                    correlations[key] = float(corr_val)
        
        return {
            "matrix": correlation_matrix.to_dict(),
            "strong_correlations": dict(sorted(correlations.items(), 
                                             key=lambda x: abs(x[1]), 
                                             reverse=True))
        }
    
    def _detect_anomalies(self) -> Dict[str, Any]:
        """Detect outliers using IQR method"""
        anomalies = {}
        
        for col in self.numeric_cols:
            col_data = self.df[col].dropna()
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = self.df[(self.df[col] < lower_bound) | (self.df[col] > upper_bound)]
            
            if len(outliers) > 0:
                anomalies[col] = {
                    "outlier_count": int(len(outliers)),
                    "outlier_percentage": float((len(outliers) / len(self.df)) * 100),
                    "bounds": {
                        "lower": float(lower_bound),
                        "upper": float(upper_bound)
                    }
                }
        
        return anomalies
    
    def detect_structure(self) -> Dict[str, Any]:
        """Detect and classify data structure"""
        structure = {
            "column_types": {},
            "time_series_columns": [],
            "numeric_columns": self.numeric_cols,
            "categorical_columns": self.categorical_cols,
            "datetime_columns": self.datetime_cols,
            "patterns": self._detect_patterns()
        }
        
        # Classify each column
        for col in self.df.columns:
            if col in self.numeric_cols:
                structure["column_types"][col] = "numeric"
            elif col in self.datetime_cols:
                structure["column_types"][col] = "datetime"
                structure["time_series_columns"].append(col)
            else:
                structure["column_types"][col] = "categorical"
        
        return structure
    
    def _detect_patterns(self) -> Dict[str, Any]:
        """Detect patterns in data"""
        patterns = {}
        
        # Check for potential ID columns (high cardinality, unique)
        for col in self.categorical_cols:
            series = self.df[col].dropna().astype(str)
            if len(series) == 0:
                continue

            unique_ratio = series.nunique() / len(series)
            avg_len = float(series.str.len().mean())
            name = str(col).strip().lower()

            # Treat as ID-like only when values are highly unique and token-like.
            # This avoids misclassifying natural-language text columns as IDs.
            looks_id_name = name == 'id' or name.endswith('_id') or 'id' in name.split('_')
            looks_token_values = avg_len <= 32

            if unique_ratio > 0.9 and (looks_id_name or looks_token_values):
                patterns[col] = "potential_id_column"
        
        return patterns
    
    def generate_charts_data(self) -> Dict[str, Any]:
        """Generate data for frontend charts"""
        charts = {}
        
        # 1. Numeric distributions
        if self.numeric_cols:
            charts["distributions"] = {}
            for col in self.numeric_cols[:5]:  # Limit to 5 to avoid overload
                hist_data = self.df[col].value_counts(bins=20).sort_index()
                charts["distributions"][col] = {
                    "labels": [str(x) for x in hist_data.index],
                    "values": hist_data.values.tolist()
                }
        
        # 2. Top categorical values
        if self.categorical_cols:
            charts["top_categories"] = {}
            for col in self.categorical_cols[:3]:  # Limit to 3
                top_vals = self.df[col].value_counts().head(10)
                charts["top_categories"][col] = {
                    "labels": top_vals.index.tolist(),
                    "values": top_vals.values.tolist()
                }
        
        # 3. Correlation heatmap data
        if len(self.numeric_cols) >= 2:
            corr_matrix = self.df[self.numeric_cols].corr()
            charts["correlation_matrix"] = {
                "columns": self.numeric_cols,
                "data": corr_matrix.values.tolist()
            }
        
        # 4. Time series if datetime columns exist
        if self.datetime_cols and self.numeric_cols:
            charts["time_series"] = {}
            time_col = self.datetime_cols[0]
            numeric_col = self.numeric_cols[0]
            
            ts_data = self.df[[time_col, numeric_col]].sort_values(time_col)
            charts["time_series"][f"{numeric_col}_over_time"] = {
                "dates": ts_data[time_col].astype(str).tolist(),
                "values": ts_data[numeric_col].tolist()
            }
        
        return charts
