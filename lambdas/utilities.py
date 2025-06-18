def get_bucket_and_key_from_s3_uri(s3_uri: str) -> (str, str):
    """
    Extracts the bucket name and key from an S3 URI.

    Args:
        s3_uri (str): The S3 URI in the format 's3://bucket-name/key'.

    Returns:
        tuple: A tuple containing the bucket name and key.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI format. Must start with 's3://'.")

    s3_uri = s3_uri[5:]  # Remove 's3://'
    parts = s3_uri.split("/", 1)

    if len(parts) != 2:
        raise ValueError("Invalid S3 URI format. Must contain a bucket and a key.")

    bucket_name = parts[0]
    key = parts[1]

    return bucket_name, key


def get_bucket_and_key_from_event(event: dict) -> (str, str):
    """
    Extracts the bucket name and key from an event that contains an S3 URI.

    Args:
        event (dict): The event containing the S3 URI.

    Returns:
        tuple: A tuple containing the bucket name and key.
    """
    if 's3_input_uri' not in event:
        raise KeyError("Event must contain 's3_input_uri' key.")

    s3_uri = event['s3_input_uri']

    return get_bucket_and_key_from_s3_uri(s3_uri)
