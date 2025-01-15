resource "aws_s3_bucket" "exo5_spark_bucket" {
  bucket = "exo5-unique-bucket"  
  tags   = local.tags
}