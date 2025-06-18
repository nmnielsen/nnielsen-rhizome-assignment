import awswrangler as wr
from lambdas.utilities import get_bucket_and_key_from_s3_uri
from lambdas.observation_validator import ObservationValidator
from lambdas.observation_filterer import ObservationFilterer
from lambdas.observation_formatter import ObservationFormatter


def generate_output_s3_uri(input_s3_uri, new_prefix):
    bucket, key = get_bucket_and_key_from_s3_uri(input_s3_uri)
    key_parts = key.split('/')
    key_parts[-2] = new_prefix
    new_key = '/'.join(key_parts)
    return f"s3://{bucket}/{new_key}"


def lambda_handler_validator(event, context):
    input_s3_uri = event['s3_input_uri']
    output_s3_uri = generate_output_s3_uri(input_s3_uri, new_prefix="validated")

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    validator = ObservationValidator(df)
    validity_df = validator.validate()

    wr.s3.to_parquet(validity_df, path=output_s3_uri, index=False)


def lambda_handler_filterer(event, context):
    input_s3_uri = event['s3_input_uri']
    output_s3_uri = generate_output_s3_uri(input_s3_uri, new_prefix="filtered")

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    filterer = ObservationFilterer(df)
    filtered_df = filterer.filter()

    wr.s3.to_parquet(filtered_df, path=output_s3_uri, index=False)


def lambda_handler_formatter(event, context):
    input_s3_uri = event['s3_input_uri']
    output_s3_uri = generate_output_s3_uri(input_s3_uri, new_prefix="formatted")

    df = wr.s3.read_csv(input_s3_uri) if input_s3_uri.endswith('.csv') else wr.s3.read_parquet(input_s3_uri)

    formatter = ObservationFormatter(df)
    formatted_df = formatter.format()

    wr.s3.to_parquet(formatted_df, path=output_s3_uri, index=False)
