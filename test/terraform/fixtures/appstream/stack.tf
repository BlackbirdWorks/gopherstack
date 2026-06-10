resource "aws_appstream_stack" "this" {
  name         = "{{.StackName}}"
  description  = "gopherstack terraform test stack"
  display_name = "Integ Stack"

  storage_connectors {
    connector_type = "HOMEFOLDERS"
  }

  user_settings {
    action     = "CLIPBOARD_COPY_FROM_LOCAL_DEVICE"
    permission = "ENABLED"
  }

  user_settings {
    action     = "CLIPBOARD_COPY_TO_LOCAL_DEVICE"
    permission = "ENABLED"
  }

  user_settings {
    action     = "FILE_UPLOAD"
    permission = "ENABLED"
  }

  user_settings {
    action     = "FILE_DOWNLOAD"
    permission = "ENABLED"
  }

  user_settings {
    action     = "PRINTING_TO_LOCAL_DEVICE"
    permission = "ENABLED"
  }

  tags = {
    Environment = "test"
  }
}
