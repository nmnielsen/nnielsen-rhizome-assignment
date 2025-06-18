import json
import os

import awswrangler as wr
import pandas as pd

from observation_handlers import log_invocation_details, logger
from model_data_builder import ModelDFBuilder
import model_s3_interface
from model_trainer import ModelTrainer
from utilities import get_bucket_and_key_from_s3_uri


@log_invocation_details
def model_data_builder(event, context):
    outcome_s3_uri = event.get('outcome_s3_uri', None)
    if not outcome_s3_uri:
        start_date = event['start_date']
        end_date = event['end_date']
        outcome_df = pd.DataFrame(
            columns=['outcome_of_int'],
            index=pd.date_range(start=pd.to_datetime(start_date), end=pd.to_datetime(end_date), freq='D')
        )
    else:
        outcome_df = wr.s3.read_parquet(outcome_s3_uri)

    observation_s3_uris_by_station_id = event['observation_s3_uris_by_station_id']
    resolution_days = event.get('resolution_days', 1)
    run_timestamp = event['run_timestamp']
    location_name = event['location_name']

    if not observation_s3_uris_by_station_id:
        raise ValueError("No observation S3 URIs provided.")

    observation_dfs_by_station_id = {
        station_id: wr.s3.read_parquet(s3_uri)
        for station_id, s3_uri in observation_s3_uris_by_station_id.items()
    }

    model_df_builder = ModelDFBuilder(
        outcome_df=outcome_df,
        observation_dfs_by_station_id=observation_dfs_by_station_id
    )
    model_df = model_df_builder.build_model_df(resolution_days=resolution_days)

    output_s3_uri = f's3://{os.environ["OUTPUT_BUCKET"]}/model_data/location={location_name}/run_timestamp={run_timestamp}/resolution={resolution_days}.parquet'

    wr.s3.to_parquet(model_df, path=output_s3_uri, index=False)

    return output_s3_uri


@log_invocation_details
def model_trainer(event, context):
    model_data_df = wr.s3.read_parquet(event['model_data_s3_uri'])
    model_type = event.get('model_type', 'random_forest')

    model_params = event.get('model_params', {})

    this_model_trainer = ModelTrainer(
        model_type=model_type,
        model_params=model_params
    )
    model, prediction_results_df, metrics = this_model_trainer.train_and_evaluate(
        df=model_data_df,
        target_col='outcome_of_int',
        test_split_date=event['test_split_date'],
        features=event.get('features', None)
    )

    logger.info(f"Model metrics: {metrics}")
    model_s3_interface.save_model_to_s3(
        model=model,
        bucket_name=os.environ["MODEL_BUCKET"],
        s3_key=f"models/{event['location_name']}/{event['run_timestamp']}/model_type={model_type}/model.pkl"
    )
    prediction_results_s3_uri = f"s3://{os.environ['MODEL_BUCKET']}/prediction_results/location={event['location_name']}/run_timestamp={event['run_timestamp']}/results.parquet"
    wr.s3.to_parquet(prediction_results_df, path=prediction_results_s3_uri, index=False)

    return {
        "statusCode": 200,
        "body": json.dumps("Model training completed successfully.")
    }


@log_invocation_details
def model_runner(event, context):
    model_s3_uri = event['model_s3_uri']
    model_data_s3_uri = event['model_data_s3_uri']
    start_date = event['start_date']
    end_date = event['end_date']

    # Read model_data_df from S3
    logger.info(f"Reading model_data_df from {model_data_s3_uri}")
    model_data_df = wr.s3.read_parquet(model_data_s3_uri)

    # Read the model from S3
    logger.info(f"Loading model from {model_s3_uri}")
    model_bucket, model_key = get_bucket_and_key_from_s3_uri(model_s3_uri)
    model = model_s3_interface.load_model_from_s3(
        bucket_name=model_bucket,
        s3_key=model_key
    )

    # Predict outcome_of_int
    logger.info("Generating predictions")
    y_pred = model.predict(model_data_df.drop(columns=['outcome_of_int'], errors='ignore'))

    # Save predictions to S3
    model_prefix = model_s3_uri.replace('/model.joblib', '')
    output_s3_key = f"{model_prefix}/predictions/{start_date}_to_{end_date}/predictions.parquet"
    output_s3_uri = f"s3://{os.environ['MODEL_BUCKET']}/{output_s3_key}"

    logger.info(f"Saving predictions to {output_s3_uri}")
    prediction_results_df = pd.DataFrame({
        'y_pred': y_pred
    }, index=model_data_df.index)
    wr.s3.to_parquet(prediction_results_df, path=output_s3_uri, index=True)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Predictions saved successfully.",
            "predictions_s3_uri": output_s3_uri
        })
    }
