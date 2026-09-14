package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_LineageAndAutoML covers sagemaker's next-highest-priority
// typed-client-uncovered op families (gopherstack-n3zi): lineage
// (Action/Context/Artifact/Association/LineageGroup), AutoML (V1+V2) extras,
// compilation job extras, monitoring job definition extras (all four
// families), monitoring alert extras and workteam extras.
func TestRealClient_LineageAndAutoML(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testLineageExtrasRealClient, "lineage_extras"},
		{testAutoMLExtrasRealClient, "automl_extras"},
		{testCompilationJobExtrasRealClient, "compilation_job_extras"},
		{testMonitoringJobDefinitionExtrasRealClient, "monitoring_job_definition_extras"},
		{testMonitoringAlertExtrasRealClient, "monitoring_alert_extras"},
		{testWorkteamExtrasRealClient, "workteam_extras"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testLineageExtrasRealClient covers UpdateAction, DeleteAction,
// UpdateContext, DeleteContext, DescribeContext, UpdateArtifact,
// DeleteAssociation, ListAssociations, DescribeLineageGroup,
// GetLineageGroupPolicy.
func testLineageExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	actionOut, err := client.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
		ActionName: aws.String("slice20-action"),
		ActionType: aws.String("ModelDeployment"),
		Source:     &smtypes.ActionSource{SourceUri: aws.String("s3://bucket/action")},
		Status:     smtypes.ActionStatusInProgress,
	})
	require.NoError(t, err)

	updActionOut, err := client.UpdateAction(t.Context(), &sagemakersdk.UpdateActionInput{
		ActionName: aws.String("slice20-action"),
		Status:     smtypes.ActionStatusCompleted,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(actionOut.ActionArn), aws.ToString(updActionOut.ActionArn))

	delActionOut, err := client.DeleteAction(t.Context(), &sagemakersdk.DeleteActionInput{
		ActionName: aws.String("slice20-action"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(actionOut.ActionArn), aws.ToString(delActionOut.ActionArn))

	ctxOut, err := client.CreateContext(t.Context(), &sagemakersdk.CreateContextInput{
		ContextName: aws.String("slice20-context"),
		ContextType: aws.String("Endpoint"),
		Source:      &smtypes.ContextSource{SourceUri: aws.String("s3://bucket/context")},
	})
	require.NoError(t, err)

	updCtxOut, err := client.UpdateContext(t.Context(), &sagemakersdk.UpdateContextInput{
		ContextName: aws.String("slice20-context"),
		Description: aws.String("updated context"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(ctxOut.ContextArn), aws.ToString(updCtxOut.ContextArn))

	descCtxOut, err := client.DescribeContext(t.Context(), &sagemakersdk.DescribeContextInput{
		ContextName: aws.String("slice20-context"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated context", aws.ToString(descCtxOut.Description))
	assert.Equal(t, "Endpoint", aws.ToString(descCtxOut.ContextType))

	artOut, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactType: aws.String("Model"),
		Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/artifact")},
	})
	require.NoError(t, err)

	updArtOut, err := client.UpdateArtifact(t.Context(), &sagemakersdk.UpdateArtifactInput{
		ArtifactArn:  artOut.ArtifactArn,
		ArtifactName: aws.String("renamed-artifact"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(artOut.ArtifactArn), aws.ToString(updArtOut.ArtifactArn))

	_, err = client.AddAssociation(t.Context(), &sagemakersdk.AddAssociationInput{
		SourceArn:       ctxOut.ContextArn,
		DestinationArn:  artOut.ArtifactArn,
		AssociationType: smtypes.AssociationEdgeTypeContributedTo,
	})
	require.NoError(t, err)

	listAssocOut, err := client.ListAssociations(t.Context(), &sagemakersdk.ListAssociationsInput{
		SourceArn: ctxOut.ContextArn,
	})
	require.NoError(t, err)
	require.Len(t, listAssocOut.AssociationSummaries, 1)
	assert.Equal(
		t,
		aws.ToString(artOut.ArtifactArn),
		aws.ToString(listAssocOut.AssociationSummaries[0].DestinationArn),
	)

	_, err = client.DeleteAssociation(t.Context(), &sagemakersdk.DeleteAssociationInput{
		SourceArn:      ctxOut.ContextArn,
		DestinationArn: artOut.ArtifactArn,
	})
	require.NoError(t, err)

	descLGOut, err := client.DescribeLineageGroup(
		t.Context(),
		&sagemakersdk.DescribeLineageGroupInput{
			LineageGroupName: aws.String("sagemaker-default-lineage-group"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "sagemaker-default-lineage-group", aws.ToString(descLGOut.LineageGroupName))

	// No PutLineageGroupPolicy exists in this backend (or in real AWS's
	// public API -- resource policies are attached out-of-band), so the
	// default lineage group never has one attached: an honest
	// ResourceNotFound, not a bug.
	_, err = client.GetLineageGroupPolicy(t.Context(), &sagemakersdk.GetLineageGroupPolicyInput{
		LineageGroupName: aws.String("sagemaker-default-lineage-group"),
	})
	require.Error(t, err)

	delCtxOut, err := client.DeleteContext(t.Context(), &sagemakersdk.DeleteContextInput{
		ContextName: aws.String("slice20-context"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(ctxOut.ContextArn), aws.ToString(delCtxOut.ContextArn))
}

// testAutoMLExtrasRealClient covers DescribeAutoMLJob, StopAutoMLJob,
// ListAutoMLJobs, ListCandidatesForAutoMLJob, DescribeAutoMLJobV2.
func testAutoMLExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateAutoMLJob(t.Context(), &sagemakersdk.CreateAutoMLJobInput{
		AutoMLJobName: aws.String("slice20-automl"),
		InputDataConfig: []smtypes.AutoMLChannel{
			{
				TargetAttributeName: aws.String("target"),
				DataSource: &smtypes.AutoMLDataSource{
					S3DataSource: &smtypes.AutoMLS3DataSource{
						S3DataType: smtypes.AutoMLS3DataTypeS3Prefix,
						S3Uri:      aws.String("s3://bucket/input/"),
					},
				},
			},
		},
		OutputDataConfig: &smtypes.AutoMLOutputDataConfig{
			S3OutputPath: aws.String("s3://bucket/output/"),
		},
		RoleArn: aws.String("arn:aws:iam::000000000000:role/sagemaker"),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeAutoMLJob(t.Context(), &sagemakersdk.DescribeAutoMLJobInput{
		AutoMLJobName: aws.String("slice20-automl"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice20-automl", aws.ToString(descOut.AutoMLJobName))

	listOut, err := client.ListAutoMLJobs(t.Context(), &sagemakersdk.ListAutoMLJobsInput{
		NameContains: aws.String("slice20-automl"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.AutoMLJobSummaries, 1)

	candOut, err := client.ListCandidatesForAutoMLJob(
		t.Context(),
		&sagemakersdk.ListCandidatesForAutoMLJobInput{
			AutoMLJobName: aws.String("slice20-automl"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, candOut.Candidates)

	_, err = client.StopAutoMLJob(t.Context(), &sagemakersdk.StopAutoMLJobInput{
		AutoMLJobName: aws.String("slice20-automl"),
	})
	require.NoError(t, err)

	_, err = client.CreateAutoMLJobV2(t.Context(), &sagemakersdk.CreateAutoMLJobV2Input{
		AutoMLJobName: aws.String("slice20-automl-v2"),
		AutoMLJobInputDataConfig: []smtypes.AutoMLJobChannel{
			{
				DataSource: &smtypes.AutoMLDataSource{
					S3DataSource: &smtypes.AutoMLS3DataSource{
						S3DataType: smtypes.AutoMLS3DataTypeS3Prefix,
						S3Uri:      aws.String("s3://bucket/input/"),
					},
				},
			},
		},
		AutoMLProblemTypeConfig: &smtypes.AutoMLProblemTypeConfigMemberTabularJobConfig{
			Value: smtypes.TabularJobConfig{TargetAttributeName: aws.String("target")},
		},
		OutputDataConfig: &smtypes.AutoMLOutputDataConfig{
			S3OutputPath: aws.String("s3://bucket/output/"),
		},
		RoleArn: aws.String("arn:aws:iam::000000000000:role/sagemaker"),
	})
	require.NoError(t, err)

	descV2Out, err := client.DescribeAutoMLJobV2(
		t.Context(),
		&sagemakersdk.DescribeAutoMLJobV2Input{
			AutoMLJobName: aws.String("slice20-automl-v2"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice20-automl-v2", aws.ToString(descV2Out.AutoMLJobName))
	assert.Equal(
		t,
		smtypes.AutoMLProblemTypeConfigNameTabular,
		descV2Out.AutoMLProblemTypeConfigName,
	)
}

// testCompilationJobExtrasRealClient covers DeleteCompilationJob,
// StopCompilationJob, ListCompilationJobs.
func testCompilationJobExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	_, err := client.CreateCompilationJob(t.Context(), &sagemakersdk.CreateCompilationJobInput{
		CompilationJobName: aws.String("slice20-compile"),
		OutputConfig: &smtypes.OutputConfig{
			S3OutputLocation: aws.String("s3://bucket/output/"),
			TargetDevice:     smtypes.TargetDeviceMlM5,
		},
		RoleArn:           aws.String("arn:aws:iam::000000000000:role/sagemaker"),
		StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(3600)},
		ModelPackageVersionArn: aws.String(
			"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg/1",
		),
	})
	require.NoError(t, err)

	listOut, err := client.ListCompilationJobs(t.Context(), &sagemakersdk.ListCompilationJobsInput{
		NameContains: aws.String("slice20-compile"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.CompilationJobSummaries, 1)

	_, err = client.StopCompilationJob(t.Context(), &sagemakersdk.StopCompilationJobInput{
		CompilationJobName: aws.String("slice20-compile"),
	})
	require.NoError(t, err)

	_, err = client.DeleteCompilationJob(t.Context(), &sagemakersdk.DeleteCompilationJobInput{
		CompilationJobName: aws.String("slice20-compile"),
	})
	require.NoError(t, err)
}

// monitoringJobDefFixtureInputs returns a minimal request shape shared by
// all four Create*JobDefinition ops -- each type differs only in which
// AppSpecification/JobInput/JobOutputConfig struct type is used, per
// minimalJobDefinitionFixture (handler_monitoring_job_definitions_test.go).
func monitoringJobDefFixtureResources() *smtypes.MonitoringResources {
	return &smtypes.MonitoringResources{
		ClusterConfig: &smtypes.MonitoringClusterConfig{
			InstanceCount:  aws.Int32(1),
			InstanceType:   smtypes.ProcessingInstanceTypeMlM5Large,
			VolumeSizeInGB: aws.Int32(10),
		},
	}
}

// testMonitoringJobDefinitionExtrasRealClient covers
// DescribeDataQualityJobDefinition, DeleteDataQualityJobDefinition,
// ListDataQualityJobDefinitions, DescribeModelBiasJobDefinition,
// DeleteModelBiasJobDefinition, ListModelBiasJobDefinitions,
// DescribeModelQualityJobDefinition, DeleteModelQualityJobDefinition,
// ListModelQualityJobDefinitions, DescribeModelExplainabilityJobDefinition,
// DeleteModelExplainabilityJobDefinition, ListModelExplainabilityJobDefinitions.
func testMonitoringJobDefinitionExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	role := aws.String("arn:aws:iam::000000000000:role/monitor")
	endpointInput := &smtypes.EndpointInput{
		EndpointName: aws.String("slice20-endpoint"),
		LocalPath:    aws.String("/opt/ml/input"),
	}
	outCfg := &smtypes.MonitoringOutputConfig{MonitoringOutputs: []smtypes.MonitoringOutput{}}

	_, err := client.CreateDataQualityJobDefinition(
		t.Context(),
		&sagemakersdk.CreateDataQualityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-dq"),
			RoleArn:           role,
			JobResources:      monitoringJobDefFixtureResources(),
			DataQualityAppSpecification: &smtypes.DataQualityAppSpecification{
				ImageUri: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/monitor:latest"),
			},
			DataQualityJobInput:        &smtypes.DataQualityJobInput{EndpointInput: endpointInput},
			DataQualityJobOutputConfig: outCfg,
		},
	)
	require.NoError(t, err)

	descDQOut, err := client.DescribeDataQualityJobDefinition(
		t.Context(),
		&sagemakersdk.DescribeDataQualityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-dq"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice20-dq", aws.ToString(descDQOut.JobDefinitionName))

	listDQOut, err := client.ListDataQualityJobDefinitions(
		t.Context(),
		&sagemakersdk.ListDataQualityJobDefinitionsInput{NameContains: aws.String("slice20-dq")},
	)
	require.NoError(t, err)
	require.Len(t, listDQOut.JobDefinitionSummaries, 1)

	_, err = client.DeleteDataQualityJobDefinition(
		t.Context(),
		&sagemakersdk.DeleteDataQualityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-dq"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateModelBiasJobDefinition(
		t.Context(),
		&sagemakersdk.CreateModelBiasJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-bias"),
			RoleArn:           role,
			JobResources:      monitoringJobDefFixtureResources(),
			ModelBiasAppSpecification: &smtypes.ModelBiasAppSpecification{
				ImageUri:  aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/bias:latest"),
				ConfigUri: aws.String("s3://bucket/bias-config.json"),
			},
			ModelBiasJobInput: &smtypes.ModelBiasJobInput{
				EndpointInput: endpointInput,
				GroundTruthS3Input: &smtypes.MonitoringGroundTruthS3Input{
					S3Uri: aws.String("s3://bucket/ground-truth/"),
				},
			},
			ModelBiasJobOutputConfig: outCfg,
		},
	)
	require.NoError(t, err)

	descBiasOut, err := client.DescribeModelBiasJobDefinition(
		t.Context(),
		&sagemakersdk.DescribeModelBiasJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-bias"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice20-bias", aws.ToString(descBiasOut.JobDefinitionName))

	listBiasOut, err := client.ListModelBiasJobDefinitions(
		t.Context(),
		&sagemakersdk.ListModelBiasJobDefinitionsInput{NameContains: aws.String("slice20-bias")},
	)
	require.NoError(t, err)
	require.Len(t, listBiasOut.JobDefinitionSummaries, 1)

	_, err = client.DeleteModelBiasJobDefinition(
		t.Context(),
		&sagemakersdk.DeleteModelBiasJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-bias"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateModelQualityJobDefinition(
		t.Context(),
		&sagemakersdk.CreateModelQualityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-quality"),
			RoleArn:           role,
			JobResources:      monitoringJobDefFixtureResources(),
			ModelQualityAppSpecification: &smtypes.ModelQualityAppSpecification{
				ImageUri: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/quality:latest"),
			},
			ModelQualityJobInput: &smtypes.ModelQualityJobInput{
				EndpointInput: endpointInput,
				GroundTruthS3Input: &smtypes.MonitoringGroundTruthS3Input{
					S3Uri: aws.String("s3://bucket/ground-truth/"),
				},
			},
			ModelQualityJobOutputConfig: outCfg,
		},
	)
	require.NoError(t, err)

	descQualOut, err := client.DescribeModelQualityJobDefinition(
		t.Context(),
		&sagemakersdk.DescribeModelQualityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-quality"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice20-quality", aws.ToString(descQualOut.JobDefinitionName))

	listQualOut, err := client.ListModelQualityJobDefinitions(
		t.Context(),
		&sagemakersdk.ListModelQualityJobDefinitionsInput{
			NameContains: aws.String("slice20-quality"),
		},
	)
	require.NoError(t, err)
	require.Len(t, listQualOut.JobDefinitionSummaries, 1)

	_, err = client.DeleteModelQualityJobDefinition(
		t.Context(),
		&sagemakersdk.DeleteModelQualityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-quality"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateModelExplainabilityJobDefinition(
		t.Context(), &sagemakersdk.CreateModelExplainabilityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-explain"),
			RoleArn:           role,
			JobResources:      monitoringJobDefFixtureResources(),
			ModelExplainabilityAppSpecification: &smtypes.ModelExplainabilityAppSpecification{
				ImageUri: aws.String(
					"123456789012.dkr.ecr.us-east-1.amazonaws.com/explain:latest",
				),
				ConfigUri: aws.String("s3://bucket/explain-config.json"),
			},
			ModelExplainabilityJobInput: &smtypes.ModelExplainabilityJobInput{
				EndpointInput: endpointInput,
			},
			ModelExplainabilityJobOutputConfig: outCfg,
		},
	)
	require.NoError(t, err)

	descExplainOut, err := client.DescribeModelExplainabilityJobDefinition(
		t.Context(),
		&sagemakersdk.DescribeModelExplainabilityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-explain"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice20-explain", aws.ToString(descExplainOut.JobDefinitionName))

	listExplainOut, err := client.ListModelExplainabilityJobDefinitions(
		t.Context(),
		&sagemakersdk.ListModelExplainabilityJobDefinitionsInput{
			NameContains: aws.String("slice20-explain"),
		},
	)
	require.NoError(t, err)
	require.Len(t, listExplainOut.JobDefinitionSummaries, 1)

	_, err = client.DeleteModelExplainabilityJobDefinition(
		t.Context(),
		&sagemakersdk.DeleteModelExplainabilityJobDefinitionInput{
			JobDefinitionName: aws.String("slice20-explain"),
		},
	)
	require.NoError(t, err)
}

// testMonitoringAlertExtrasRealClient covers UpdateMonitoringAlert,
// ListMonitoringAlerts, ListMonitoringAlertHistory, ListMonitoringExecutions.
func testMonitoringAlertExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	schedOut, err := client.CreateMonitoringSchedule(
		t.Context(),
		&sagemakersdk.CreateMonitoringScheduleInput{
			MonitoringScheduleName:   aws.String("slice20-schedule"),
			MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{},
		},
	)
	require.NoError(t, err)

	updAlertOut, err := client.UpdateMonitoringAlert(
		t.Context(),
		&sagemakersdk.UpdateMonitoringAlertInput{
			MonitoringScheduleName: aws.String("slice20-schedule"),
			MonitoringAlertName:    aws.String("slice20-alert"),
			DatapointsToAlert:      aws.Int32(1),
			EvaluationPeriod:       aws.Int32(1),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(schedOut.MonitoringScheduleArn),
		aws.ToString(updAlertOut.MonitoringScheduleArn),
	)

	listAlertsOut, err := client.ListMonitoringAlerts(
		t.Context(),
		&sagemakersdk.ListMonitoringAlertsInput{
			MonitoringScheduleName: aws.String("slice20-schedule"),
		},
	)
	require.NoError(t, err)
	require.Len(t, listAlertsOut.MonitoringAlertSummaries, 1)
	assert.Equal(
		t,
		"slice20-alert",
		aws.ToString(listAlertsOut.MonitoringAlertSummaries[0].MonitoringAlertName),
	)

	historyOut, err := client.ListMonitoringAlertHistory(
		t.Context(),
		&sagemakersdk.ListMonitoringAlertHistoryInput{
			MonitoringScheduleName: aws.String("slice20-schedule"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, historyOut.MonitoringAlertHistory)

	execOut, err := client.ListMonitoringExecutions(
		t.Context(),
		&sagemakersdk.ListMonitoringExecutionsInput{
			MonitoringScheduleName: aws.String("slice20-schedule"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, execOut.MonitoringExecutionSummaries)
}

// testWorkteamExtrasRealClient covers UpdateWorkteam,
// DescribeSubscribedWorkteam (honestly not-found), ListSubscribedWorkteams,
// ListLabelingJobsForWorkteam, EnableSagemakerServicecatalogPortfolio,
// DisableSagemakerServicecatalogPortfolio,
// GetSagemakerServicecatalogPortfolioStatus, ListResourceCatalogs.
func testWorkteamExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)

	teamOut, err := client.CreateWorkteam(t.Context(), &sagemakersdk.CreateWorkteamInput{
		WorkteamName: aws.String("slice20-team"),
		Description:  aws.String("test team"),
		MemberDefinitions: []smtypes.MemberDefinition{
			{CognitoMemberDefinition: &smtypes.CognitoMemberDefinition{
				ClientId:  aws.String("client-1"),
				UserPool:  aws.String("pool-1"),
				UserGroup: aws.String("group-1"),
			}},
		},
	})
	require.NoError(t, err)

	updOut, err := client.UpdateWorkteam(t.Context(), &sagemakersdk.UpdateWorkteamInput{
		WorkteamName: aws.String("slice20-team"),
		Description:  aws.String("updated team"),
	})
	require.NoError(t, err)
	require.NotNil(t, updOut.Workteam)
	assert.Equal(t, "updated team", aws.ToString(updOut.Workteam.Description))

	_, err = client.DescribeSubscribedWorkteam(
		t.Context(),
		&sagemakersdk.DescribeSubscribedWorkteamInput{
			WorkteamArn: teamOut.WorkteamArn,
		},
	)
	require.Error(t, err)

	listSubOut, err := client.ListSubscribedWorkteams(
		t.Context(),
		&sagemakersdk.ListSubscribedWorkteamsInput{},
	)
	require.NoError(t, err)
	assert.Empty(t, listSubOut.SubscribedWorkteams)

	listJobsOut, err := client.ListLabelingJobsForWorkteam(
		t.Context(),
		&sagemakersdk.ListLabelingJobsForWorkteamInput{
			WorkteamArn: teamOut.WorkteamArn,
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, listJobsOut.LabelingJobSummaryList)

	_, err = client.EnableSagemakerServicecatalogPortfolio(
		t.Context(), &sagemakersdk.EnableSagemakerServicecatalogPortfolioInput{},
	)
	require.NoError(t, err)

	statusOut, err := client.GetSagemakerServicecatalogPortfolioStatus(
		t.Context(), &sagemakersdk.GetSagemakerServicecatalogPortfolioStatusInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, smtypes.SagemakerServicecatalogStatusEnabled, statusOut.Status)

	_, err = client.DisableSagemakerServicecatalogPortfolio(
		t.Context(), &sagemakersdk.DisableSagemakerServicecatalogPortfolioInput{},
	)
	require.NoError(t, err)

	statusOut2, err := client.GetSagemakerServicecatalogPortfolioStatus(
		t.Context(), &sagemakersdk.GetSagemakerServicecatalogPortfolioStatusInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, smtypes.SagemakerServicecatalogStatusDisabled, statusOut2.Status)

	catalogsOut, err := client.ListResourceCatalogs(
		t.Context(),
		&sagemakersdk.ListResourceCatalogsInput{},
	)
	require.NoError(t, err)
	assert.NotNil(t, catalogsOut.ResourceCatalogs)

	metaOut, err := client.ListModelMetadata(t.Context(), &sagemakersdk.ListModelMetadataInput{})
	require.NoError(t, err)
	assert.NotNil(t, metaOut.ModelMetadataSummaries)
}
