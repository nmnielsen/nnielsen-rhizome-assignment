from functools import wraps
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log_validation(func):
    @wraps(func)
    def wrapper(df: pd.DataFrame, col_name: str):
        result = func(df, col_name)
        invalid_pct = 100 * (~result).mean()
        message = f"{func.__name__}: {col_name} → {invalid_pct:.2f}% invalid"
        logger.info(message)
        print(message)
        return result
    return wrapper


@log_validation
def check_missing(df: pd.DataFrame, col_name: str) -> pd.Series:
    return ~df.loc[:, col_name].isnull()


def check_extreme_values(min_val: float, max_val: float):
    @log_validation
    @wraps(check_extreme_values)
    def _check(df: pd.DataFrame, col_name: str) -> pd.Series:
        series = df.loc[:, col_name]
        return series.between(min_val, max_val) | series.isnull()
    return _check


def check_valid_binary_digits(length: int):
    @log_validation
    @wraps(check_valid_binary_digits)
    def _check(df: pd.DataFrame, col_name: str) -> pd.Series:
        series = df.loc[:, col_name]
        return series.astype(str).str.fullmatch(f'[01]{{{length}}}') | series.isnull()
    return _check


def check_greater_than(other_col: str):
    """
    Returns a validation function that checks if each value in the column
    is greater than the value in the specified other column.
    """
    @log_validation
    @wraps(check_valid_binary_digits)
    def _check(df: pd.DataFrame, col_name: str) -> pd.Series:
        series = df.loc[:, col_name]
        if other_col not in df.columns:
            raise ValueError(f"Column '{other_col}' does not exist in the DataFrame.")
        else:
            other_series = df.loc[:, other_col]
        return (series > other_series) | series.isnull() | other_series.isnull()
    return _check


class ObservationValidator:
    def __init__(self, df: pd.DataFrame):
        self.df = df

        self.validation_map = {
            'TEMP':    [check_extreme_values(-30, 110), check_greater_than('MIN'), check_missing],
            'DEWP':    [check_extreme_values(0, 110), check_missing],
            'MAX':     [check_extreme_values(-30, 130), check_greater_than('TEMP'), check_missing],
            'MIN':     [check_extreme_values(-50, 110), check_missing],
            'SLP':     [check_extreme_values(870, 1085), check_missing],
            'STP':     [check_extreme_values(870, 1085), check_missing],
            'WDSP':    [check_extreme_values(0, 150), check_missing],
            'MXSPD':   [check_extreme_values(0, 200)],
            'GUST':    [check_extreme_values(0, 250)],
            'VISIB':   [check_extreme_values(0, 100), check_missing],
            'SNDP':    [check_extreme_values(0, 100)],
            'PRCP':    [check_extreme_values(0, 500), check_missing],
            'FRSHTT':  [check_valid_binary_digits(6), check_missing]
        }

    def validate(self) -> pd.DataFrame:
        validity = pd.DataFrame(True, index=self.df.index, columns=self.df.columns)

        for col, checks in self.validation_map.items():
            if col in self.df.columns:
                for check in checks:
                    result = check(df=self.df, col_name=col)
                    validity[col] &= result

        return validity
