import os
import pandas as pd
from model_data_builder import ModelDFBuilder
from model_trainer import ModelTrainer
import pickle

# Define paths for local files
local_outcome_file = "data/synthetic_data.parquet"
local_observation_files = {
    "kpdx": "data/kpdx.csv",
    "ksle": "data/ksle.csv",
}
local_model_file = "models/trained_model.pkl"
local_predictions_file = "results/predictions.parquet"

start_date = "2022-01-01"
end_date = "2022-12-31"
predictions_file = "results/final_predictions.parquet"


# Step 1: Run Model Data Builder
def run_model_data_builder(outcome_file, observation_files, resolution_days=1):
    # Load outcome data
    if os.path.exists(outcome_file):
        outcome_df = pd.read_parquet(outcome_file)
    else:
        raise FileNotFoundError(f"Outcome file not found: {outcome_file}")

    # Load observation data
    observation_dfs_by_station = {
        station_id: pd.read_parquet(file_path)
        for station_id, file_path in observation_files.items()
    }

    # Build the model data
    model_df_builder = ModelDFBuilder(outcome_df, observation_dfs_by_station)
    model_data_df = model_df_builder.build_model_df(resolution_days=resolution_days)

    # Save the model data locally
    model_data_file = "data/model_data.parquet"
    model_data_df.to_parquet(model_data_file, index=False)
    print(f"Model data saved to: {model_data_file}")
    return model_data_file


# Step 2: Run Model Trainer
def run_model_trainer(model_data_file, model_type="random_forest", test_split_date="2023-01-01", model_params=None):
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
    prediction_results_df.to_parquet(local_predictions_file, index=False)
    print(f"Predictions saved to: {local_predictions_file}")

    print(f"Model metrics: {metrics}")
    return local_model_file, local_predictions_file


# Step 3: Run Model Runner
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


# Step 1: Build model data
model_data_file = run_model_data_builder(local_outcome_file, local_observation_files)

# Step 2: Train the model
model_file, model_training_predictions_file = run_model_trainer(model_data_file, test_split_date="2023-01-01")

# Step 3: Run the model runner
final_predictions_file = run_model_runner(
    model_file=model_file,
    model_data_file=model_data_file,
    start_date=start_date,
    end_date=end_date,
    prediction_file=predictions_file
)
