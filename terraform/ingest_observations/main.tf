# Create S3 bucket for observation files
resource "aws_s3_bucket" "observation_files" {
  bucket = "observation-files"
}

# IAM Role for Lambda functions
resource "aws_iam_role" "lambda_role" {
  name = "lambda-s3-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "s3-access-policy"
  description = "Policy to allow Lambda functions to access S3 bucket"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:PutObject"]
        Effect   = "Allow"
        Resource = "${aws_s3_bucket.observation_files.arn}/*"
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# Lambda functions
module "validator_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "validator_lambda"
  handler       = "handlers.lambda_handler_validator"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../../lambdas/"
}

module "filterer_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "filterer_lambda"
  handler       = "handlers.lambda_handler_filterer"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../../lambdas/"
}

module "formatter_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "formatter_lambda"
  handler       = "handlers.lambda_handler_formatter"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../../lambdas/"
}

# Step Function
data "template_file" "step_function_definition" {
  template = file("${path.module}/ingest_observations_state_machine.json")

  vars = {
    formatter_lambda_arn = module.formatter_lambda.lambda_function_arn
    filterer_lambda_arn   = module.filterer_lambda.lambda_function_arn
  }
}

resource "aws_sfn_state_machine" "observation_step_function" {
  name     = "observation-step-function"
  role_arn = aws_iam_role.lambda_role.arn
  definition = data.template_file.step_function_definition.rendered
}

# S3 Event to trigger Step Function
resource "aws_s3_bucket_notification" "s3_event" {
  bucket = aws_s3_bucket.observation_files.id

  lambda_function {
    lambda_function_arn = aws_sfn_state_machine.observation_step_function.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
}
