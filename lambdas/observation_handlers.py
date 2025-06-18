import json
import logging
import os

import awswrangler as wr
import boto3

from utilities import get_bucket_and_key_from_s3_uri
from observation_validator import ObservationValidator
from observation_filterer import ObservationFilterer
from observation_formatter import ObservationFormatter


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


STATION_IDS_BY_LOCATION_NAME = {
    "oregon1": ["KPDX", "KSLE"]
}


def generate_observation_s3_uri(prefix, station_id, bucket=None, input_s3_uri=None):
    assert bucket or input_s3_uri, "Either bucket or input_s3_uri must be provided."

    if bucket is None:
        bucket, _ = get_bucket_and_key_from_s3_uri(input_s3_uri)

    return f"s3://{bucket}/{prefix}/station_id={station_id}/data.parquet"


def log_invocation_details(func):
    def wrapper(event, context):
        logger.info(f'Invoked with {event}')
        return func(event, context)
    return wrapper


@log_invocation_details
def observation_validator(event, context):
    input_s3_uri = event['input_s3_uri']
    station_id = event['station_id']
    output_s3_uri = generate_observation_s3_uri(
        input_s3_uri=input_s3_uri,
        prefix="validated",
        station_id=station_id
    )

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    validator = ObservationValidator(df)
    validity_df = validator.validate()

    error_rate = validator.calculate_percent_of_rows_with_errors(validity_df)
    logger.info(f"Total error rate: {error_rate:.2f}%")

    wr.s3.to_parquet(validity_df, path=output_s3_uri)

    return {
        's3_uri': output_s3_uri,
        'error_rate': error_rate
    }


@log_invocation_details
def observation_filterer(event, context):
    input_s3_uri = event['input_s3_uri']
    validation_result_s3_uri = event.get('validation_result_s3_uri', None)
    station_id = event['station_id']
    output_s3_uri = generate_observation_s3_uri(
        input_s3_uri=input_s3_uri,
        prefix="filtered",
        station_id=station_id
    )

    observation_df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)
    validation_df = wr.s3.read_parquet(validation_result_s3_uri) if validation_result_s3_uri else None

    filterer = ObservationFilterer(
        df=observation_df,
        validation_df=validation_df
    )
    filtered_df = filterer.filter()

    wr.s3.to_parquet(filtered_df, path=output_s3_uri)

    return output_s3_uri


@log_invocation_details
def observation_formatter(event, context):
    input_s3_uri = event['input_s3_uri']
    station_id = event['station_id']
    output_s3_uri = generate_observation_s3_uri(
        input_s3_uri=input_s3_uri,
        prefix="formatted",
        station_id=station_id
    )

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    formatter = ObservationFormatter(df)
    formatted_df = formatter.format()

    wr.s3.to_parquet(formatted_df, path=output_s3_uri)

    return output_s3_uri


@log_invocation_details
def step_function_invoker(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    station_id = object_key.split('/')[-1].split('.')[0]

    # Step Function input
    step_function_input = {
        "input_s3_uri": f"s3://{bucket_name}/{object_key}",
        "station_id": station_id
    }

    stepfunctions_client = boto3.client('stepfunctions')

    # Start Step Function execution
    response = stepfunctions_client.start_execution(
        stateMachineArn=os.environ['STEP_FUNCTION_ARN'],
        input=json.dumps(step_function_input)
    )

    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }


@log_invocation_details
def relevant_observation_s3_uri_by_station_assembler(event, context):
    """
    Assembles a dictionary of S3 URIs for observations by station ID.

    Args:
        event (dict): The event containing the S3 URIs and station IDs.

    Returns:
        dict: A dictionary mapping station IDs to their corresponding S3 URIs.
    """
    location_name = event['location_name']
    station_ids = STATION_IDS_BY_LOCATION_NAME.get(location_name, [])

    observation_s3_uri_by_station_id = {
        station_id: generate_observation_s3_uri(
            bucket=os.environ["OBSERVATION_BUCKET"],
            prefix="observations",
            station_id=station_id
        )
        for station_id in station_ids
    }

    return observation_s3_uri_by_station_id


