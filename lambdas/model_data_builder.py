from typing import Dict

import pandas as pd


class ModelDFBuilder:
    BASE_FEATURES = [
        'DEWP',
        'FRSHTT',
        'GUST',
        'MAX',
        'MIN',
        'MXSPD',
        'PRCP',
        'SLP',
        'SNDP',
        'STP',
        'TEMP',
        'VISIB',
        'WDSP'
    ]
    WINDOW_DAYS = [
        3,
        7,
        14,
        30
    ]
    ROLLING_MEAN_SUFFIX = 'd_mean'

    def __init__(self, outcome_df: pd.DataFrame, observation_dfs_by_station_id: Dict[str, pd.DataFrame]):
        self.outcome_df = outcome_df
        self.observation_dfs_by_station = observation_dfs_by_station_id

        self.outcome_column_name = outcome_df.columns[0]

    def build_model_df(self, resolution_days: int = 1) -> pd.DataFrame:
        """
        Combines observations and outcome series into a single DataFrame.
        Computes additional features based on the base features and specified window days.
        Resamples the data to the specified resolution in days.
        """
        model_df = self.combine_obs_with_outcome()
        model_df = self.change_model_data_resolution(model_df, resolution_days)

        return model_df

    def combine_obs_with_outcome(self):
        self.outcome_df.sort_index(ascending=True, inplace=True)
        start_date, end_date = self.outcome_df.index[0], self.outcome_df.index[-1]
        dfs = [self.outcome_df]
        for station, obs_df in self.observation_dfs_by_station.items():
            obs_df = self.compute_additional_features(obs_df)

            feature_columns = [
                c for c in obs_df.columns if c in self.BASE_FEATURES or self.ROLLING_MEAN_SUFFIX in c
            ]
            feature_df = obs_df.loc[start_date:end_date, feature_columns]
            feature_df.columns = [f'{c}_{station}' for c in feature_df.columns]
            dfs.append(feature_df)

        model_data_df = pd.concat(dfs, axis=1)
        model_data_df.loc[:, self.outcome_column_name] = model_data_df.loc[:, self.outcome_column_name].fillna(0)

        return model_data_df

    def compute_additional_features(self, obs_df: pd.DataFrame) -> pd.DataFrame:
        for f in self.BASE_FEATURES:
            for d in self.WINDOW_DAYS:
                new_feature = f'{f}_{d}{self.ROLLING_MEAN_SUFFIX}'
                obs_df[new_feature] = obs_df[f].rolling(window=d, min_periods=1).mean()
        return obs_df

    @staticmethod
    def change_model_data_resolution(df: pd.DataFrame, resolution_days: int) -> pd.DataFrame:
        return df.resample(f'{resolution_days}D').sum()
