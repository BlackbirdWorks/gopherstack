resource "aws_s3_bucket" "quicksight" {
  bucket = "qskr-quicksight-bucket"
}

resource "aws_vpc" "quicksight" {
  cidr_block = "10.140.0.0/16"

  tags = {
    Name = "qskr-quicksight-vpc"
  }
}

resource "aws_subnet" "quicksight" {
  vpc_id            = aws_vpc.quicksight.id
  cidr_block        = "10.140.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "qskr-quicksight-subnet-a"
  }
}

resource "aws_subnet" "quicksight2" {
  vpc_id            = aws_vpc.quicksight.id
  cidr_block        = "10.140.2.0/24"
  availability_zone = "us-east-1b"

  tags = {
    Name = "qskr-quicksight-subnet-b"
  }
}

resource "aws_security_group" "quicksight" {
  name   = "qskr-quicksight-sg"
  vpc_id = aws_vpc.quicksight.id
}

resource "aws_iam_role" "quicksight" {
  name = "qskr-quicksight-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "quicksight" {
  name = "qskr-quicksight-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["quicksight:DescribeDashboard"]
      Resource = "*"
    }]
  })
}

resource "aws_quicksight_namespace" "example" {
  aws_account_id = "000000000000"
  namespace      = "qskr-namespace"
}

resource "aws_quicksight_group" "example" {
  aws_account_id = "000000000000"
  group_name     = "qskr-group"
}

resource "aws_quicksight_user" "example" {
  aws_account_id = "000000000000"
  user_name      = "qskr-user"
  email          = "qskr-user@example.com"
  identity_type  = "QUICKSIGHT"
  user_role      = "READER"
}

resource "aws_quicksight_group_membership" "example" {
  aws_account_id = "000000000000"
  group_name     = aws_quicksight_group.example.group_name
  member_name    = aws_quicksight_user.example.user_name
}

resource "aws_quicksight_role_membership" "example" {
  aws_account_id = "000000000000"
  role           = "READER"
  member_name    = aws_quicksight_group.example.group_name
}

resource "aws_quicksight_data_source" "example" {
  aws_account_id = "000000000000"
  data_source_id = "qskr-data-source"
  name           = "qskr-data-source"

  parameters {
    s3 {
      manifest_file_location {
        bucket = aws_s3_bucket.quicksight.bucket
        key    = "manifest.json"
      }
    }
  }

  type = "S3"
}

resource "aws_quicksight_data_set" "example" {
  aws_account_id = "000000000000"
  data_set_id    = "qskr-data-set"
  name           = "qskr-data-set"
  import_mode    = "SPICE"

  physical_table_map {
    physical_table_map_id = "table1"

    s3_source {
      data_source_arn = aws_quicksight_data_source.example.arn

      input_columns {
        name = "col1"
        type = "STRING"
      }

      upload_settings {
        format = "JSON"
      }
    }
  }
}

resource "aws_quicksight_ingestion" "example" {
  aws_account_id = "000000000000"
  data_set_id    = aws_quicksight_data_set.example.data_set_id
  ingestion_id   = "qskr-ingestion"
  ingestion_type = "FULL_REFRESH"
}

resource "aws_quicksight_refresh_schedule" "example" {
  aws_account_id = "000000000000"
  data_set_id    = aws_quicksight_data_set.example.data_set_id
  schedule_id    = "qskr-refresh-schedule"

  schedule {
    schedule_frequency {
      interval = "DAILY"
    }

    refresh_type = "FULL_REFRESH"
  }
}

resource "aws_quicksight_analysis" "example" {
  aws_account_id = "000000000000"
  analysis_id    = "qskr-analysis"
  name           = "qskr-analysis"

  definition {
    data_set_identifiers_declarations {
      data_set_arn = aws_quicksight_data_set.example.arn
      identifier   = "dataset1"
    }

    sheets {
      sheet_id = "sheet1"
      title    = "Sheet 1"
    }
  }
}

resource "aws_quicksight_template" "example" {
  aws_account_id      = "000000000000"
  template_id         = "qskr-template"
  name                = "qskr-template"
  version_description = "v1"

  source_entity {
    source_analysis {
      arn = aws_quicksight_analysis.example.arn

      data_set_references {
        data_set_arn         = aws_quicksight_data_set.example.arn
        data_set_placeholder = "dataset1"
      }
    }
  }
}

resource "aws_quicksight_template_alias" "example" {
  aws_account_id          = "000000000000"
  template_id             = aws_quicksight_template.example.template_id
  alias_name              = "qskr-alias"
  template_version_number = aws_quicksight_template.example.version_number
}

resource "aws_quicksight_dashboard" "example" {
  aws_account_id      = "000000000000"
  dashboard_id        = "qskr-dashboard"
  name                = "qskr-dashboard"
  version_description = "v1"

  source_entity {
    source_template {
      arn = aws_quicksight_template.example.arn

      data_set_references {
        data_set_arn         = aws_quicksight_data_set.example.arn
        data_set_placeholder = "dataset1"
      }
    }
  }
}

resource "aws_quicksight_theme" "example" {
  aws_account_id = "000000000000"
  theme_id       = "qskr-theme"
  name           = "qskr-theme"
  base_theme_id  = "MIDNIGHT"

  configuration {
    data_color_palette {
      colors = [
        "#FF0000", "#00FF00", "#0000FF", "#FFFF00",
        "#FF00FF", "#00FFFF", "#FFFFFF", "#000000",
      ]
    }
  }
}

resource "aws_quicksight_folder" "example" {
  aws_account_id = "000000000000"
  folder_id      = "qskr-folder"
  name           = "qskr-folder"
}

resource "aws_quicksight_folder_membership" "example" {
  aws_account_id = "000000000000"
  folder_id      = aws_quicksight_folder.example.folder_id
  member_id      = aws_quicksight_data_set.example.data_set_id
  member_type    = "DATASET"
}

resource "aws_quicksight_iam_policy_assignment" "example" {
  aws_account_id    = "000000000000"
  assignment_name   = "qskr-iam-policy-assignment"
  assignment_status = "ENABLED"
  policy_arn        = aws_iam_policy.quicksight.arn

  identities {
    user = [aws_quicksight_user.example.user_name]
  }
}

resource "aws_quicksight_vpc_connection" "example" {
  aws_account_id     = "000000000000"
  vpc_connection_id  = "qskr-vpc-connection"
  name               = "qskr-vpc-connection"
  role_arn           = aws_iam_role.quicksight.arn
  security_group_ids = [aws_security_group.quicksight.id]
  subnet_ids         = [aws_subnet.quicksight.id, aws_subnet.quicksight2.id]
}
