resource "aws_appconfig_application" "this" {
  name = "{{.AppName}}"
}

resource "aws_appconfig_environment" "this" {
  name           = "production"
  application_id = aws_appconfig_application.this.id
}

resource "aws_appconfig_configuration_profile" "this" {
  application_id = aws_appconfig_application.this.id
  name           = "my-config"
  location_uri   = "hosted"
}

resource "aws_appconfig_deployment_strategy" "this" {
  name                           = "my-strategy-{{.AppName}}"
  deployment_duration_in_minutes = 0
  final_bake_time_in_minutes     = 0
  growth_factor                  = 100
  replicate_to                   = "NONE"
}
