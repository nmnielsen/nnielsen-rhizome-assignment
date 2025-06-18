# Create S3 bucket for observation files
resource "aws_s3_bucket" "observation_files" {
  bucket = "${var.project}-observation-files"
}

# IAM Role for Lambda functions
resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-lambda-role"
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

resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project}-lambda-policy"
  description = "Policy to allow Lambda functions to access S3 bucket"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:PutObject"]
        Effect   = "Allow"
        Resource = "${aws_s3_bucket.observation_files.arn}/*"
      },
      {
        Action   = "lambda:GetLayerVersion",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = "states:StartExecution",
        Resource = aws_sfn_state_machine.observation_step_function.arn
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Lambda functions
module "validator_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "validator_lambda"
  handler       = "observation_handlers.observation_validator"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../lambdas/"
  timeout = 180
  memory_size = 1024

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
}

module "filterer_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "filterer_lambda"
  handler       = "observation_handlers.observation_filterer"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../lambdas/"
  timeout = 180
  memory_size = 1024

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
}

module "formatter_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "formatter_lambda"
  handler       = "observation_handlers.observation_formatter"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../lambdas/"

  layers = [
      "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
}

module "step_function_invoker_lambda" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "observation_ingest_step_function_invoker_lambda"
  handler       = "observation_handlers.step_function_invoker"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_role.arn
  source_path = "../lambdas/"
  timeout = 180
  memory_size = 1024

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
  environment_variables = {
    STEP_FUNCTION_ARN = aws_sfn_state_machine.observation_step_function.arn
  }
}

# Step Function
data "template_file" "step_function_definition" {
  template = file("${path.module}/ingest_observations_state_machine.json")

  vars = {
    formatter_lambda_arn = module.formatter_lambda.lambda_function_arn
    filterer_lambda_arn   = module.filterer_lambda.lambda_function_arn
    validator_lambda_arn = module.validator_lambda.lambda_function_arn
  }
}

resource "aws_sfn_state_machine" "observation_step_function" {
  name     = "observation-step-function"
  role_arn = aws_iam_role.lambda_role.arn
  definition = data.template_file.step_function_definition.rendered
}

# S3 Event to trigger Step Function
resource "aws_lambda_permission" "allow_s3_invocation" {
  statement_id  = "AllowS3Invocation"
  action        = "lambda:InvokeFunction"
  function_name = module.step_function_invoker_lambda.lambda_function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.observation_files.arn
}

resource "aws_s3_bucket_notification" "s3_event" {
  bucket = aws_s3_bucket.observation_files.id

  lambda_function {
    lambda_function_arn = module.step_function_invoker_lambda.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
}
