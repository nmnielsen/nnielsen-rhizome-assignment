import os
import pandas as pd
from model_data_builder import ModelDFBuilder
from model_trainer import ModelTrainer
from observation_formatter import ObservationFormatter
from observation_validator import ObservationValidator
from observation_filterer import ObservationFilterer
import pickle

# Define paths for local files
local_outcome_file = "data/synthetic_data.parquet"
local_observation_files = {
    "kpdx": "data/kpdx.csv",
    "ksle": "data/ksle.csv",
}
local_filtered_observation_files = {
    "kpdx": "data/filtered_kpdx.parquet",
    "ksle": "data/filtered_ksle.parquet",
}
local_model_file = "models/trained_model.pkl"
local_predictions_file = "results/predictions.parquet"

test_split_date = "2021-06-01"

prediction_start_date = "2022-01-01"
prediction_end_date = "2022-12-31"
predictions_file = "results/final_predictions.parquet"


# Step 1: Format, validate, and filter observations
def process_observations(observation_files):
    filtered_files = {}
    for station_id, file_path in observation_files.items():
        # Format observations
        df = pd.read_csv(file_path) if file_path.endswith(".csv") else pd.read_parquet(file_path)
        formatter = ObservationFormatter(df)
        formatted_df = formatter.format()

        # Validate observations
        validator = ObservationValidator(formatted_df)
        validity_df = validator.validate()
        error_rate = validator.calculate_percent_of_rows_with_errors(validity_df)
        print(f"Validation error rate for {station_id}: {error_rate:.2f}%")

        # Filter observations
        filterer = ObservationFilterer(df=formatted_df, validation_df=validity_df)
        filtered_df = filterer.filter()

        # Save filtered observations locally
        filtered_file_path = f"data/filtered_{station_id}.parquet"
        filtered_df.to_parquet(filtered_file_path)
        filtered_files[station_id] = filtered_file_path
        print(f"Filtered observations saved to: {filtered_file_path}")

    return filtered_files


# Step 2: Run Model Data Builder
def run_model_data_builder(observation_files, outcome_file=None, start_date=None, end_date=None, resolution_days=1):
    # Load outcome data
    if not outcome_file:
        outcome_df = pd.DataFrame(
            columns=['outcome_of_int'],
            index=pd.date_range(start=pd.to_datetime(start_date), end=pd.to_datetime(end_date), freq='D')
        )
    elif os.path.exists(outcome_file):
        outcome_df = pd.read_parquet(outcome_file)
        outcome_df.date = pd.to_datetime(outcome_df.date)
        outcome_df = outcome_df.groupby('date').outcome_of_int.sum().to_frame()
        outcome_df = outcome_df.reindex(pd.date_range(start=outcome_df.index.min(), end=outcome_df.index.max(), freq='D'), fill_value=0)
    else:
        raise FileNotFoundError(f"Outcome file not found: {outcome_file}")

    # Load filtered observation data
    observation_dfs_by_station = {
        station_id: pd.read_parquet(file_path)
        for station_id, file_path in observation_files.items()
    }

    # Build the model data
    model_df_builder = ModelDFBuilder(outcome_df, observation_dfs_by_station)
    model_data_df = model_df_builder.build_model_df(resolution_days=resolution_days)

    # Save the model data locally
    model_data_file = "data/model_data.parquet"
    model_data_df.to_parquet(model_data_file)
    print(f"Model data saved to: {model_data_file}")
    return model_data_file


# Step 3: Run Model Trainer
def run_model_trainer(model_data_file, test_split_date, model_type="random_forest", model_params=None):
    # Load model data
    model_data_df = pd.read_parquet(model_data_file)

    # Train the model
    trainer = ModelTrainer(model_type=model_type, model_params=model_params)
    model, prediction_results_df, metrics = trainer.train_and_evaluate(
        df=model_data_df,
        target_col="outcome_of_int",
        test_split_date=test_split_date
    )

    # Save the trained model locally
    with open(local_model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"Trained model saved to: {local_model_file}")

    # Save the predictions locally
    prediction_results_df.to_parquet(local_predictions_file)
    print(f"Predictions saved to: {local_predictions_file}")

    print(f"Model metrics: {metrics}")
    return local_model_file, local_predictions_file


# Step 4: Run Model Runner
def run_model_runner(model_file, model_data_file,
                     start_date, end_date, prediction_file):
    # Load the model
    with open(model_file, "rb") as f:
        model = pickle.load(f)

    # Load the model data
    model_data_df = pd.read_parquet(model_data_file)

    # Generate predictions
    input_df = model_data_df.drop(columns=["outcome_of_int"], errors="ignore")
    input_df = input_df.loc[pd.to_datetime(start_date):pd.to_datetime(end_date)]
    y_pred = model.predict(input_df)

    # Save predictions locally
    prediction_results_df = pd.DataFrame({"y_pred": y_pred}, index=model_data_df.index)
    prediction_results_df.to_parquet(predictions_file, index=True)
    print(f"Final predictions saved to: {predictions_file}")
    return predictions_file


# Main execution
if __name__ == "__main__":
    # Step 1: Process observations
    filtered_observation_files = process_observations(local_observation_files)

    # Step 2: Build model data
    model_data_file = run_model_data_builder(
        observation_files=filtered_observation_files,
        outcome_file=local_outcome_file
    )

    # Step 3: Train the model
    model_file, model_training_predictions_file = run_model_trainer(
        model_data_file=model_data_file,
        test_split_date=test_split_date
    )

    # Step 4: Build prediction data
    prediction_data = run_model_data_builder(
        observation_files=filtered_observation_files,
        start_date=prediction_start_date,
        end_date=prediction_end_date
    )

    # Step 4: Run the model runner
    final_predictions_file = run_model_runner(
        model_file=model_file,
        model_data_file=model_data_file,
        start_date=prediction_start_date,
        end_date=prediction_end_date,
        prediction_file=predictions_file
    )
