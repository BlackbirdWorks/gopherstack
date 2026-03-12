resource "aws_identitystore_user" "this" {
  identity_store_id = "{{.IdentityStoreID}}"
  user_name         = "{{.UserName}}"
  display_name      = "{{.DisplayName}}"

  name {
    given_name  = "{{.GivenName}}"
    family_name = "{{.FamilyName}}"
  }
}

resource "aws_identitystore_group" "this" {
  identity_store_id = "{{.IdentityStoreID}}"
  display_name      = "{{.GroupName}}"
  description       = "Terraform test group"
}

resource "aws_identitystore_group_membership" "this" {
  identity_store_id = "{{.IdentityStoreID}}"
  group_id          = aws_identitystore_group.this.group_id
  member_id         = aws_identitystore_user.this.user_id
}
