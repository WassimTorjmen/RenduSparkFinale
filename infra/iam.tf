# Politique qui permet à Glue d'assumer le rôle
data "aws_iam_policy_document" "exo5_spark_glue_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# Politique IAM qui donne l'accès à S3 et KMS
data "aws_iam_policy_document" "exo5_spark_glue_role_policy" {
  statement {
    actions = [
      "s3:*",
      "kms:*",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::exo5-unique-bucket/*",  
      "arn:aws:s3:::exo5-unique-bucket"     
    ]
  }
}

# Création de la politique IAM basée sur le document précédent
resource "aws_iam_policy" "exo5_spark_glue_role_policy" {
  name   = "exo5_spark_glue_policy"
  path   = "/"
  policy = data.aws_iam_policy_document.exo5_spark_glue_role_policy.json
}

# Création du rôle IAM pour Glue
resource "aws_iam_role" "exo5_spark_glue" {
  name                = "exo5_spark_glue_role"
  assume_role_policy  = data.aws_iam_policy_document.exo5_spark_glue_assume_role_policy.json
  managed_policy_arns = [aws_iam_policy.exo5_spark_glue_role_policy.arn]
}
