resource "aws_elastictranscoder_pipeline" "this" {
  name          = "tf-et-pipeline-{{.PipelineName}}"
  input_bucket  = "my-input-bucket"
  output_bucket = "my-output-bucket"
  role          = "arn:aws:iam::000000000000:role/Elastic_Transcoder_Default_Role"
}
