resource "aws_codepipeline" "this" {
  name     = "tf-pipeline-{{.Suffix}}"
  role_arn = "arn:aws:iam::000000000000:role/pipeline-role"

  artifact_store {
    location = "my-artifact-bucket"
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeCommit"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = {
        RepositoryName = "my-repo"
        BranchName     = "main"
      }
    }
  }

  stage {
    name = "Deploy"

    action {
      name            = "Deploy"
      category        = "Deploy"
      owner           = "AWS"
      provider        = "CloudFormation"
      version         = "1"
      input_artifacts = ["source_output"]

      configuration = {
        ActionMode    = "CREATE_UPDATE"
        StackName     = "my-stack"
        TemplatePath  = "source_output::template.yaml"
      }
    }
  }

  tags = {
    Environment = "test"
  }
}
