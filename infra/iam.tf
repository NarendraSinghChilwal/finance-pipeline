# IAM User for the Python generator/uploader script
resource "aws_iam_user" "finance_pipeline_loader" {
  name = "finance-pipeline-loader"
}

# Policy document — scoped to this bucket only, only the verbs the script needs
resource "aws_iam_policy" "finance_pipeline_loader" {
  name = "finance-pipeline-loader-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject"
        ]
        Resource = "arn:aws:s3:::finance-pipeline-ingest-2026/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::finance-pipeline-ingest-2026"
      }
    ]
  })
}

# Attach the policy to the user
resource "aws_iam_user_policy_attachment" "finance_pipeline_loader" {
  user       = aws_iam_user.finance_pipeline_loader.name
  policy_arn = aws_iam_policy.finance_pipeline_loader.arn
}

# Generate access keys for the user — Terraform will store these in state
resource "aws_iam_access_key" "finance_pipeline_loader" {
  user = aws_iam_user.finance_pipeline_loader.name
}

# Output the keys so you can copy them into .env
output "loader_access_key_id" {
  value = aws_iam_access_key.finance_pipeline_loader.id
}

output "loader_secret_access_key" {
  value     = aws_iam_access_key.finance_pipeline_loader.secret
  sensitive = true
}