provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "model_files" {
  bucket = "${var.project}-model-and-run-files"
}

resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project}-training-lambda-execution-role"
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
  name        = "${var.project}-model-lambda-policy"
  description = "Policy to allow Lambda functions to access S3 bucket"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:PutObject"]
        Effect   = "Allow"
        Resource = "${aws_s3_bucket.model_files.arn}/*"
      },
      {
        Action   = "lambda:GetLayerVersion",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = "states:StartExecution",
        Resource = aws_sfn_state_machine.step_function.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

module "relevant_observation_assembler" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "relevant_observation_assembler"
  handler       = "model_handlers.relevant_observation_assembler"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_execution_role.arn
  source_path = "../lambdas/"

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
  environment_variables = {
    MODEL_BUCKET = aws_s3_bucket.model_files.bucket
  }
}

module "model_data_builder" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "model_data_builder"
  handler       = "model_handlers.model_data_builder"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_execution_role.arn
  source_path = "../lambdas/"

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
  environment_variables = {
    MODEL_BUCKET = aws_s3_bucket.model_files.bucket
  }
}

module "model_trainer" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "model_trainer"
  handler       = "model_handlers.model_trainer"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_execution_role.arn
  source_path = "../lambdas/"

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
  environment_variables = {
    MODEL_BUCKET = aws_s3_bucket.model_files.bucket
  }
}

module "model_runner" {
  source = "terraform-aws-modules/lambda/aws"
  function_name = "model_runner"
  handler       = "model_handlers.model_runner"
  runtime       = "python3.9"
  policy          = aws_iam_role.lambda_execution_role.arn
  source_path = "../lambdas/"

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:29"
  ]
  environment_variables = {
    MODEL_BUCKET = aws_s3_bucket.model_files.bucket
  }
}

data "template_file" "model_trainer_step_function_definition" {
  template = file("${path.module}/model_training_state_machine.json")

  vars = {
    relevant_observation_assembler_arn = module.relevant_observation_assembler.lambda_function_arn
    model_data_builder_arn             = module.model_data_builder.lambda_function_arn
    model_trainer_arn                  = module.model_trainer.lambda_function_arn
  }
}

resource "aws_sfn_state_machine" "step_function" {
  name     = "${var.project}-model-training"
  role_arn = aws_iam_role.lambda_execution_role.arn
  definition = data.template_file.model_trainer_step_function_definition.rendered
}

data "template_file" "model_runner_step_function_definition" {
  template = file("${path.module}/model_running_state_machine.json")

  vars = {
    relevant_observation_assembler_arn = module.relevant_observation_assembler.lambda_function_arn
    model_data_builder_arn             = module.model_data_builder.lambda_function_arn
    model_runner_arn                  = module.model_runner.lambda_function_arn
  }
}

resource "aws_sfn_state_machine" "model_runner_step_function" {
  name     = "${var.project}-model-runner"
  role_arn = aws_iam_role.lambda_execution_role.arn
  definition = data.template_file.model_runner_step_function_definition.rendered
}
