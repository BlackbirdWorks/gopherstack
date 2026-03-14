resource "aws_s3tables_table_bucket" "this" {
  name = "tf-s3t-{{.Suffix}}"
}

resource "aws_s3tables_namespace" "this" {
  namespace        = "tfns{{.Suffix}}"
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
}

resource "aws_s3tables_table" "this" {
  namespace        = aws_s3tables_namespace.this.namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  name             = "tf-table-{{.Suffix}}"
  format           = "ICEBERG"
}
