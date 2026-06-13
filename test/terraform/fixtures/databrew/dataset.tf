resource "aws_s3_bucket" "data" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

resource "aws_glue_databrew_dataset" "this" {
  name = "{{.DatasetName}}"

  input {
    s3_input_definition {
      bucket = aws_s3_bucket.data.bucket
      key    = "data.csv"
    }
  }

  format = "CSV"

  format_options {
    csv {
      delimiter = ","
    }
  }

  tags = {
    Environment = "test"
  }
}
