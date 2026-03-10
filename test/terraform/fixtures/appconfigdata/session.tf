resource "null_resource" "appconfigdata_placeholder" {
  triggers = {
    app  = "{{.AppName}}"
    env  = "{{.EnvName}}"
  }
}
