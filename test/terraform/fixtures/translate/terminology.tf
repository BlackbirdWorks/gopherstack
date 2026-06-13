resource "aws_translate_terminology" "this" {
  name           = "{{.TerminologyName}}"
  merge_strategy = "OVERWRITE"

  terminology_data {
    file   = "en,es\ngopherstack,gopherstack\nterraform,terraform\n"
    format = "CSV"
  }
}
