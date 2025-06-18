import boto3
import pickle
import os


def save_model_to_s3(model, bucket_name, s3_key):
    """
    Saves a model to S3 using pickle.

    Parameters:
    - model: The trained model to save.
    - bucket_name: The name of the S3 bucket.
    - s3_key: The S3 key (path) where the model will be saved.
    """
    # Serialize the model to a temporary file
    temp_file = "/tmp/model.pkl"
    with open(temp_file, "wb") as f:
        pickle.dump(model, f)

    # Upload the file to S3
    s3_client = boto3.client("s3")
    s3_client.upload_file(temp_file, bucket_name, s3_key)

    # Clean up the temporary file
    os.remove(temp_file)


def load_model_from_s3(bucket_name, s3_key):
    """
    Loads a model from S3 using pickle.

    Parameters:
    - bucket_name: The name of the S3 bucket.
    - s3_key: The S3 key (path) where the model is saved.

    Returns:
    - The deserialized model.
    """
    # Download the file from S3 to a temporary location
    temp_file = "/tmp/model.pkl"
    s3_client = boto3.client("s3")
    s3_client.download_file(bucket_name, s3_key, temp_file)

    # Load the model from the file
    with open(temp_file, "rb") as f:
        model = pickle.load(f)

    # Clean up the temporary file
    os.remove(temp_file)

    return model
