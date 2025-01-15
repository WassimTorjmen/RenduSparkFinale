resource "aws_glue_job" "exo5_spark_glue_job" {
  name     = "exo5-spark-job"
  role_arn = aws_iam_role.exo5_spark_glue.arn

  command {
    script_location = "s3://exo5-unique-bucket/spark-jobs/exo2_glue_job.py" # Script principal
  }

  glue_version       = "3.0"
  number_of_workers  = 2
  worker_type        = "Standard"

  default_arguments = {
    "--additional-python-modules"       = "s3://exo5-unique-bucket/wheel/spark-handson.whl" 
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--INPUT_PATH"                      = "s3://exo5-unique-bucket/input/"  
    "--OUTPUT_PATH"                     = "s3://exo5-unique-bucket/output/"
    "--src-folder-path"                 = "s3://exo5-unique-bucket/spark-jobs/src/" 
  }

  tags = local.tags
}
