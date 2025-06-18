import json
import logging
import os

import awswrangler as wr
import boto3

from lambdas.utilities import get_bucket_and_key_from_s3_uri
from lambdas.observation_validator import ObservationValidator
from lambdas.observation_filterer import ObservationFilterer
from lambdas.observation_formatter import ObservationFormatter


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_output_s3_uri(input_s3_uri, new_prefix):
    bucket, key = get_bucket_and_key_from_s3_uri(input_s3_uri)
    key_parts = key.split('/')
    key_parts[-2] = new_prefix
    new_key = '/'.join(key_parts)
    return f"s3://{bucket}/{new_key}"


def log_invocation_details(func):
    def wrapper(event, context):
        logger.info(f'Invoked with {event}')
        return func(event, context)
    return wrapper


@log_invocation_details
def observation_validator(event, context):
    input_s3_uri = event['input_s3_uri']
    output_s3_uri = generate_output_s3_uri(input_s3_uri, new_prefix="validation_results")

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    validator = ObservationValidator(df)
    validity_df = validator.validate()

    error_rate = validator.calculate_percent_of_rows_with_errors(validity_df)
    logger.info(f"Total error rate: {error_rate:.2f}%")

    wr.s3.to_parquet(validity_df, path=output_s3_uri, index=False)

    return {
        's3_uri': output_s3_uri,
        'error_rate': error_rate
    }


@log_invocation_details
def observation_filterer(event, context):
    input_s3_uri = event['input_s3_uri']
    validation_result_s3_uri = event.get('validation_result_s3_uri', None)
    output_s3_uri = generate_output_s3_uri(input_s3_uri, new_prefix="filtered")

    observation_df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)
    validation_df = wr.s3.read_parquet(validation_result_s3_uri) if validation_result_s3_uri else None

    filterer = ObservationFilterer(
        df=observation_df,
        validation_df=validation_df
    )
    filtered_df = filterer.filter()

    wr.s3.to_parquet(filtered_df, path=output_s3_uri, index=False)

    return output_s3_uri


@log_invocation_details
def observation_formatter(event, context):
    input_s3_uri = event['input_s3_uri']
    output_s3_uri = generate_output_s3_uri(input_s3_uri, new_prefix="formatted")

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    formatter = ObservationFormatter(df)
    formatted_df = formatter.format()

    wr.s3.to_parquet(formatted_df, path=output_s3_uri, index=False)

    return output_s3_uri


@log_invocation_details
def step_function_invoker(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Step Function input
    step_function_input = {
        "input_s3_uri": f"s3://{bucket_name}/{object_key}",
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
