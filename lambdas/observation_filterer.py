from lambdas.observation_validator import ObservationValidator


class ObservationFilterer:
    def __init__(self, df):
        self.df = df

    def filter(self):
        validator = ObservationValidator(self.df)
        validity_df = validator.validate()
        filtered_df = self.df.where(validity_df, other=pd.NA)

        return filtered_df
