resource "aws_organizations_organization" "this" {
  feature_set = "ALL"
}

resource "aws_organizations_organizational_unit" "dev" {
  name      = "development-{{.Suffix}}"
  parent_id = aws_organizations_organization.this.roots[0].id
}
