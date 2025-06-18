import pandas as pd


class ObservationFilterer:
    def __init__(self, df, validation_df=None):
        self.df = df
        self.validation_df = validation_df

    def filter(self):
        if self.validation_df is not None:
            filtered_df = self.df.where(self.validation_df, other=pd.NA)
        else:
            filtered_df = self.df

        return filtered_df
