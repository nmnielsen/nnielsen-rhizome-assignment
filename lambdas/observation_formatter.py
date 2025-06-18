import pandas as pd


class ObservationFormatter:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

        self.formatter_map = {
            'TEMP': [self.format_float],
            'DEWP': [self.format_float],
            'MAX': [self.format_float],
            'MIN': [self.format_float],
            'SLP': [self.format_float],
            'STP': [self.format_float],
            'WDSP': [self.format_float],
            'MXSPD': [self.format_float],
            'GUST': [self.format_float, self.replace_value({'999.9': pd.NA})],
            'VISIB': [self.format_float],
            'SNDP': [self.format_float],
            'PRCP': [self.format_float],
            'FRSHTT': [self.format_frshtt],
            'DATE': [self.format_date],
        }

    def format_float(self, df: pd.DataFrame, col_name: str) -> pd.Series:
        col = df[col_name]
        return col.round(2).where(col.notnull(), pd.NA)

    def format_frshtt(self, df: pd.DataFrame, col_name: str) -> pd.Series:
        col = df[col_name]
        return (
            col.fillna(pd.NA)
            .astype("Int64", errors="ignore")  # Handle integers safely
            .astype(str, errors="ignore")
            .str.zfill(6)
            .where(col.notnull(), pd.NA)  # Ensure null values remain consistent
        )

    def format_date(self, df: pd.DataFrame, col_name: str) -> pd.Series:
        col = df[col_name]
        return pd.to_datetime(col, errors='coerce').dt.strftime('%Y-%m-%d').where(col.notnull(), pd.NA)

    def replace_value(self, replacements: dict):
        def _replace_value(df: pd.DataFrame, col_name: str) -> pd.Series:
            return df[col_name].replace(replacements)
        return _replace_value

    def format(self) -> pd.DataFrame:
        for col, formatters in self.formatter_map.items():
            if col in self.df.columns:
                for formatter in formatters:
                    self.df[col] = formatter(df=self.df, col_name=col)

        self.df = self.df.set_index('DATE')
        self.df.index = pd.to_datetime(self.df.index, errors='coerce')
        self.df = self.df.sort_index(ascending=True)
        # expand index to include all dates in the range
        self.df = self.df.reindex(
            pd.date_range(start=self.df.index.min(), end=self.df.index.max()),
        )

        return self.df
