resource "aws_translate_terminology" "this" {
  name             = "{{.TerminologyName}}"
  merge_strategy   = "OVERWRITE"
  description      = "Terraform test terminology"

  term_list {
    source_term  = "gopherstack"
    target_term  = "gopherstack"
    source_language = "en"
    target_language = "es"
  }
}
