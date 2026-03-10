resource "terraform_data" "support" {
  input = "gopherstack-support-{{.CaseName}}"
}
