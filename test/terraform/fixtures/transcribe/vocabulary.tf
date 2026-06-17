resource "aws_transcribe_vocabulary" "this" {
  vocabulary_name = "{{.VocabName}}"
  language_code   = "en-US"
  phrases         = ["gopherstack", "terraform", "localstack"]

  tags = {
    Environment = "test"
  }
}
