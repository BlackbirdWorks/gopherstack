resource "aws_managedblockchain_member" "this" {
  network_id    = "{{.NetworkID}}"
  invitation_id = "inv-placeholder"

  member_configuration {
    name = "{{.MemberName}}"

    framework_configuration {
      fabric {
        admin_username = "admin"
        admin_password = "Password123!"
      }
    }
  }
}
