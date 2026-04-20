"""
Data processing module for cleaning and preparing datasets
"""
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any


class DataProcessor:
    """Handles automatic data cleaning and preprocessing"""
    
    def __init__(self):
        self.original_shape = None
        self.report = {}
    
    def process(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Comprehensive data processing pipeline
        
        Args:
            df: Raw pandas DataFrame
            
        Returns:
            Tuple of (cleaned DataFrame, processing report)
        """
        self.original_shape = df.shape
        self.report = {
            "original_rows": df.shape[0],
            "original_columns": df.shape[1],
            "steps": []
        }
        
        # Step 1: Remove completely empty rows and columns
        df = self._remove_empty_rows_columns(df)
        
        # Step 2: Handle duplicates
        df = self._handle_duplicates(df)
        
        # Step 3: Infer and convert data types
        df = self._infer_data_types(df)
        
        # Step 4: Handle missing values
        df = self._handle_missing_values(df)
        
        # Step 5: Clean text columns
        df = self._clean_text_columns(df)
        
        # Final report
        self.report["final_rows"] = df.shape[0]
        self.report["final_columns"] = df.shape[1]
        self.report["rows_removed"] = self.original_shape[0] - df.shape[0]
        
        return df, self.report
    
    def _remove_empty_rows_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove completely empty rows and columns"""
        initial_shape = df.shape
        
        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')
        
        # Remove completely empty rows
        df = df.dropna(axis=0, how='all')
        
        removed_cols = initial_shape[1] - df.shape[1]
        removed_rows = initial_shape[0] - df.shape[0]
        
        if removed_cols > 0 or removed_rows > 0:
            self.report["steps"].append({
                "step": "Remove Empty Rows/Columns",
                "removed_columns": removed_cols,
                "removed_rows": removed_rows
            })
        
        return df
    
    def _handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and remove exact duplicate rows"""
        initial_rows = len(df)
        df = df.drop_duplicates()
        duplicates_removed = initial_rows - len(df)
        
        if duplicates_removed > 0:
            self.report["steps"].append({
                "step": "Remove Duplicates",
                "duplicates_removed": duplicates_removed
            })
        
        return df
    
    def _infer_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Smart data type inference"""
        conversions = {}
        
        for col in df.columns:
            col_len = len(df[col])
            if col_len == 0:
                conversions[col] = str(df[col].dtype)
                continue

            # Skip if already numeric
            if pd.api.types.is_numeric_dtype(df[col]):
                conversions[col] = str(df[col].dtype)
                continue
            
            # Try to convert to numeric
            try:
                converted = pd.to_numeric(df[col], errors='coerce')
                if converted.notna().sum() / col_len > 0.8:  # 80% success rate
                    df[col] = converted
                    conversions[col] = "numeric (inferred)"
                    continue
            except:
                pass
            
            # Try to convert to datetime
            try:
                # Use mixed format parsing to avoid costly per-element fallback warnings.
                converted = pd.to_datetime(df[col], errors='coerce', format='mixed')
                if converted.notna().sum() / col_len > 0.8:
                    df[col] = converted
                    conversions[col] = "datetime (inferred)"
                    continue
            except:
                pass
            
            # Default to string/categorical
            df[col] = df[col].astype(str)
            conversions[col] = "string"
        
        if conversions:
            self.report["steps"].append({
                "step": "Infer Data Types",
                "conversions": conversions
            })
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intelligently handle missing values"""
        missing_report = {}
        
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count == 0:
                continue
            
            missing_pct = (missing_count / len(df)) * 100
            
            # If > 50% missing, drop column
            if missing_pct > 50:
                df = df.drop(columns=[col])
                missing_report[col] = f"Dropped (>{missing_pct:.1f}% missing)"
                continue
            
            # For numeric: fill with median
            if pd.api.types.is_numeric_dtype(df[col]):
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                missing_report[col] = f"Filled with median ({median_val:.2f})"
            
            # For datetime: fill with mode or drop
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].ffill()  # Forward fill
                missing_report[col] = "Filled with forward fill"
            
            # For categorical/string: fill with mode
            else:
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col] = df[col].fillna(mode_val[0])
                    missing_report[col] = f"Filled with mode ({mode_val[0]})"
        
        if missing_report:
            self.report["steps"].append({
                "step": "Handle Missing Values",
                "details": missing_report
            })
        
        return df
    
    def _clean_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean text columns (trim whitespace, lowercase, etc.)"""
        text_cols_cleaned = []
        
        for col in df.columns:
            if df[col].dtype == 'object':
                # Strip whitespace
                df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else x)
                text_cols_cleaned.append(col)
        
        if text_cols_cleaned:
            self.report["steps"].append({
                "step": "Clean Text Columns",
                "columns_cleaned": text_cols_cleaned
            })
        
        return df
