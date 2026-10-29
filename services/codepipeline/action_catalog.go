package codepipeline

import "fmt"

const (
	actionProviderCloudFormation = "CloudFormation"
	actionProviderECS            = "ECS"
	actionProviderECR            = "ECR"
	actionProviderCodeCommit     = "CodeCommit"
	actionProviderCodeStar       = "CodeStarSourceConnection"

	awsActionVersion = "1"
)

// awsActionKnown reports whether category/provider is an AWS-owned action type from the CodePipeline action
// structure reference.
func awsActionKnown(category, provider string) bool {
	switch category + "/" + provider {
	case "Source/S3", "Source/CodeCommit", "Source/ECR", "Source/CodeStarSourceConnection",
		"Build/CodeBuild", "Build/ECRBuildAndPublish",
		"Test/CodeBuild", "Test/DeviceFarm",
		"Deploy/S3", "Deploy/CloudFormation", "Deploy/CodeDeploy", "Deploy/ECS", "Deploy/CodeDeployToECS",
		"Deploy/ElasticBeanstalk", "Deploy/ServiceCatalog", "Deploy/AppConfig", "Deploy/OpsWorks", "Deploy/Alexa",
		"Deploy/CloudFormationStackSet", "Deploy/CloudFormationStackInstances", "Deploy/EKS", "Deploy/EC2",
		"Invoke/Lambda", "Invoke/StepFunctions", "Invoke/InspectorScan",
		"Approval/Manual", "Compute/Commands":
		return true
	}

	return false
}

func validCFNActionMode(mode string) bool {
	switch mode {
	case cfnModeCreateUpdate, cfnModeReplaceOnFailure, cfnModeDeleteOnly, cfnModeChangeSetReplace,
		cfnModeChangeSetExecute:
		return true
	}

	return false
}

// validateActionDeclarations rejects AWS-owned actions whose type real CodePipeline refuses with
// InvalidActionDeclarationException. Custom and ThirdParty owners are validated elsewhere.
func validateActionDeclarations(stages []Stage) error {
	for _, s := range stages {
		for _, a := range s.Actions {
			if err := validateAWSAction(s.Name, a); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateAWSAction(stage string, a Action) error {
	id := a.ActionTypeID
	if id.Owner != actionOwnerAWS {
		return nil
	}

	if !awsActionKnown(id.Category, id.Provider) || id.Version != awsActionVersion {
		return fmt.Errorf(
			"%w: ActionType (Category: '%s', Owner: '%s', Provider: '%s', Version: '%s') is not a valid action type "+
				"(action '%s' in stage '%s')",
			ErrInvalidActionDeclaration, id.Category, id.Owner, id.Provider, id.Version, a.Name, stage)
	}

	if id.Provider == actionProviderCloudFormation && id.Category == actionCategoryDeploy &&
		!validCFNActionMode(a.Configuration["ActionMode"]) {
		return fmt.Errorf("%w: Action configuration for action '%s' has invalid ActionMode '%s'",
			ErrInvalidActionDeclaration, a.Name, a.Configuration["ActionMode"])
	}

	return nil
}

// legacyUnwiredProvider reports the providers whose actions historically completed instantly when their backend seam
// is unwired; they keep that behaviour so partially wired backends still run.
func legacyUnwiredProvider(provider string) bool {
	switch provider {
	case actionProviderCodeBuild, actionProviderLambda, actionProviderCodeDeploy, actionProviderS3:
		return true
	}

	return false
}
