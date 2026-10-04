package main

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/apprunner"
	apprunnertypes "github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchtypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	codebuildtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	cptypes "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/aws-sdk-go-v2/service/configservice"
	configtypes "github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/fsx"
	fsxtypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/aws/aws-sdk-go-v2/service/guardduty"
	gdtypes "github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/aws/aws-sdk-go-v2/service/xray"
)

func platformIsolationCases() []regionCase {
	return []regionCase{
		acmCase(),
		acmpcaCase(),
		apprunnerCase(),
		batchCase(),
		codebuildCase(),
		codepipelineCase(),
		dmsCase(),
		elasticbeanstalkCase(),
		emrCase(),
		emrserverlessCase(),
		fsxCase(),
		guarddutyCase(),
		securityhubCase(),
		kinesisanalyticsv2Case(),
		sagemakerCase(),
		xrayCase(),
		transferCase(),
		route53resolverCase(),
		vpclatticeCase(),
		applicationautoscalingCase(),
		awsconfigCase(),
		cloudtrailCase(),
		daxCase(),
		elasticsearchCase(),
	}
}

func acmCase() regionCase {
	return regionCase{
		name: "acm",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := acm.NewFromConfig(cfg).RequestCertificate(ctx, &acm.RequestCertificateInput{
				DomainName: aws.String(name + ".example.com"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := acm.NewFromConfig(cfg).ListCertificates(ctx, &acm.ListCertificatesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.CertificateSummaryList))
			for _, c := range out.CertificateSummaryList {
				names = append(names, strings.TrimSuffix(aws.ToString(c.DomainName), ".example.com"))
			}

			return names, nil
		},
	}
}

func acmpcaCase() regionCase {
	return regionCase{
		name: "acmpca",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := acmpca.NewFromConfig(cfg).CreateCertificateAuthority(ctx, &acmpca.CreateCertificateAuthorityInput{
				CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
				CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
					KeyAlgorithm:     acmpcatypes.KeyAlgorithmRsa2048,
					SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withrsa,
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String(name)},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := acmpca.NewFromConfig(cfg).
				ListCertificateAuthorities(ctx, &acmpca.ListCertificateAuthoritiesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.CertificateAuthorities))
			for _, ca := range out.CertificateAuthorities {
				if ca.CertificateAuthorityConfiguration != nil && ca.CertificateAuthorityConfiguration.Subject != nil {
					names = append(names, aws.ToString(ca.CertificateAuthorityConfiguration.Subject.CommonName))
				}
			}

			return names, nil
		},
	}
}

func apprunnerCase() regionCase {
	return regionCase{
		name: "apprunner",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := apprunner.NewFromConfig(cfg).CreateService(ctx, &apprunner.CreateServiceInput{
				ServiceName: aws.String(name),
				SourceConfiguration: &apprunnertypes.SourceConfiguration{
					ImageRepository: &apprunnertypes.ImageRepository{
						ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
						ImageRepositoryType: apprunnertypes.ImageRepositoryTypeEcrPublic,
					},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := apprunner.NewFromConfig(cfg).ListServices(ctx, &apprunner.ListServicesInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.ServiceSummaryList,
				func(s apprunnertypes.ServiceSummary) *string { return s.ServiceName },
			), nil
		},
	}
}

func batchCase() regionCase {
	return regionCase{
		name: "batch",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := batch.NewFromConfig(cfg).CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{
				ComputeEnvironmentName: aws.String(name), Type: batchtypes.CETypeUnmanaged,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := batch.NewFromConfig(cfg).
				DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.ComputeEnvironments, func(c batchtypes.ComputeEnvironmentDetail) *string {
				return c.ComputeEnvironmentName
			}), nil
		},
	}
}

func codebuildCase() regionCase {
	return regionCase{
		name: "codebuild",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codebuild.NewFromConfig(cfg).CreateProject(ctx, &codebuild.CreateProjectInput{
				Name:        aws.String(name),
				ServiceRole: aws.String("arn:aws:iam::" + regionAccount + ":role/cb"),
				Source: &codebuildtypes.ProjectSource{
					Type:      codebuildtypes.SourceTypeNoSource,
					Buildspec: aws.String("version: 0.2"),
				},
				Artifacts: &codebuildtypes.ProjectArtifacts{Type: codebuildtypes.ArtifactsTypeNoArtifacts},
				Environment: &codebuildtypes.ProjectEnvironment{
					Type:        codebuildtypes.EnvironmentTypeLinuxContainer,
					Image:       aws.String("aws/codebuild/standard:7.0"),
					ComputeType: codebuildtypes.ComputeTypeBuildGeneral1Small,
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codebuild.NewFromConfig(cfg).ListProjects(ctx, &codebuild.ListProjectsInput{})
			if err != nil {
				return nil, err
			}

			return out.Projects, nil
		},
	}
}

func codepipelineCase() regionCase {
	return regionCase{
		name: "codepipeline",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codepipeline.NewFromConfig(cfg).CreatePipeline(ctx, &codepipeline.CreatePipelineInput{
				Pipeline: &cptypes.PipelineDeclaration{
					Name:    aws.String(name),
					RoleArn: aws.String("arn:aws:iam::" + regionAccount + ":role/cp"),
					ArtifactStore: &cptypes.ArtifactStore{
						Type:     cptypes.ArtifactStoreTypeS3,
						Location: aws.String("cp-artifacts"),
					},
					Stages: []cptypes.StageDeclaration{
						{Name: aws.String("Source"), Actions: []cptypes.ActionDeclaration{{
							Name: aws.String("src"),
							ActionTypeId: &cptypes.ActionTypeId{
								Category: cptypes.ActionCategorySource, Owner: cptypes.ActionOwnerAws,
								Provider: aws.String("S3"), Version: aws.String("1"),
							},
							Configuration:   map[string]string{"S3Bucket": "src", "S3ObjectKey": "k"},
							OutputArtifacts: []cptypes.OutputArtifact{{Name: aws.String("out")}},
						}}},
						{Name: aws.String("Approve"), Actions: []cptypes.ActionDeclaration{{
							Name: aws.String("gate"),
							ActionTypeId: &cptypes.ActionTypeId{
								Category: cptypes.ActionCategoryApproval, Owner: cptypes.ActionOwnerAws,
								Provider: aws.String("Manual"), Version: aws.String("1"),
							},
						}}},
					},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codepipeline.NewFromConfig(cfg).ListPipelines(ctx, &codepipeline.ListPipelinesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Pipelines, func(p cptypes.PipelineSummary) *string { return p.Name }), nil
		},
	}
}

func dmsCase() regionCase {
	return regionCase{
		name: "dms",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := databasemigrationservice.NewFromConfig(cfg).CreateEndpoint(ctx,
				&databasemigrationservice.CreateEndpointInput{
					EndpointIdentifier: aws.String(name),
					EndpointType:       dmstypes.ReplicationEndpointTypeValueSource,
					EngineName:         aws.String("mysql"),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := databasemigrationservice.NewFromConfig(cfg).DescribeEndpoints(ctx,
				&databasemigrationservice.DescribeEndpointsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Endpoints, func(e dmstypes.Endpoint) *string { return e.EndpointIdentifier }), nil
		},
	}
}

func elasticbeanstalkCase() regionCase {
	return regionCase{
		name: "elasticbeanstalk",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := elasticbeanstalk.NewFromConfig(cfg).CreateApplication(ctx,
				&elasticbeanstalk.CreateApplicationInput{ApplicationName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := elasticbeanstalk.NewFromConfig(cfg).DescribeApplications(ctx,
				&elasticbeanstalk.DescribeApplicationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Applications))
			for _, a := range out.Applications {
				names = append(names, aws.ToString(a.ApplicationName))
			}

			return names, nil
		},
	}
}

func emrCase() regionCase {
	return regionCase{
		name: "emr",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := emr.NewFromConfig(cfg).CreateSecurityConfiguration(ctx, &emr.CreateSecurityConfigurationInput{
				Name: aws.String(name), SecurityConfiguration: aws.String("{}"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := emr.NewFromConfig(cfg).ListSecurityConfigurations(ctx, &emr.ListSecurityConfigurationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.SecurityConfigurations))
			for _, s := range out.SecurityConfigurations {
				names = append(names, aws.ToString(s.Name))
			}

			return names, nil
		},
	}
}

func emrserverlessCase() regionCase {
	return regionCase{
		name: "emrserverless",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := emrserverless.NewFromConfig(cfg).CreateApplication(ctx, &emrserverless.CreateApplicationInput{
				Name: aws.String(name), ReleaseLabel: aws.String("emr-6.9.0"), Type: aws.String("SPARK"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := emrserverless.NewFromConfig(cfg).ListApplications(ctx, &emrserverless.ListApplicationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Applications))
			for _, a := range out.Applications {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func fsxCase() regionCase {
	return regionCase{
		name: "fsx",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := fsx.NewFromConfig(cfg).CreateFileSystem(ctx, &fsx.CreateFileSystemInput{
				FileSystemType:  fsxtypes.FileSystemTypeLustre,
				StorageCapacity: aws.Int32(1200),
				SubnetIds:       []string{"subnet-0123456789abcdef0"},
				Tags:            []fsxtypes.Tag{{Key: aws.String("Name"), Value: aws.String(name)}},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := fsx.NewFromConfig(cfg).DescribeFileSystems(ctx, &fsx.DescribeFileSystemsInput{})
			if err != nil {
				return nil, err
			}

			var names []string

			for _, f := range out.FileSystems {
				for _, tg := range f.Tags {
					if aws.ToString(tg.Key) == "Name" {
						names = append(names, aws.ToString(tg.Value))
					}
				}
			}

			return names, nil
		},
	}
}

func guarddutyCase() regionCase {
	return regionCase{
		name: "guardduty",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			c := guardduty.NewFromConfig(cfg)

			det, err := c.ListDetectors(ctx, &guardduty.ListDetectorsInput{})
			if err != nil {
				return err
			}

			var id string

			if len(det.DetectorIds) > 0 {
				id = det.DetectorIds[0]
			} else {
				created, cerr := c.CreateDetector(ctx, &guardduty.CreateDetectorInput{Enable: aws.Bool(true)})
				if cerr != nil {
					return cerr
				}

				id = aws.ToString(created.DetectorId)
			}

			_, err = c.CreateFilter(ctx, &guardduty.CreateFilterInput{
				DetectorId: aws.String(id), Name: aws.String(name), Action: gdtypes.FilterActionNoop,
				FindingCriteria: &gdtypes.FindingCriteria{Criterion: map[string]gdtypes.Condition{
					"severity": {GreaterThanOrEqual: aws.Int64(1)},
				}},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			c := guardduty.NewFromConfig(cfg)

			det, err := c.ListDetectors(ctx, &guardduty.ListDetectorsInput{})
			if err != nil || len(det.DetectorIds) == 0 {
				return nil, err
			}

			out, err := c.ListFilters(ctx, &guardduty.ListFiltersInput{DetectorId: aws.String(det.DetectorIds[0])})
			if err != nil {
				return nil, err
			}

			return out.FilterNames, nil
		},
	}
}

func securityhubCase() regionCase {
	return regionCase{
		name: "securityhub",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			c := securityhub.NewFromConfig(cfg)

			if _, err := c.DescribeHub(ctx, &securityhub.DescribeHubInput{}); err != nil {
				if _, err = c.EnableSecurityHub(ctx, &securityhub.EnableSecurityHubInput{}); err != nil {
					return err
				}
			}

			_, err := c.CreateActionTarget(ctx, &securityhub.CreateActionTargetInput{
				Name: aws.String(name), Description: aws.String("d"), Id: aws.String(strings.ReplaceAll(name, "-", "")),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := securityhub.NewFromConfig(cfg).
				DescribeActionTargets(ctx, &securityhub.DescribeActionTargetsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.ActionTargets))
			for _, a := range out.ActionTargets {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func kinesisanalyticsv2Case() regionCase {
	return regionCase{
		name: "kinesisanalyticsv2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := kinesisanalyticsv2.NewFromConfig(cfg).
				CreateApplication(ctx, &kinesisanalyticsv2.CreateApplicationInput{
					ApplicationName:      aws.String(name),
					RuntimeEnvironment:   kav2types.RuntimeEnvironmentFlink118,
					ServiceExecutionRole: aws.String("arn:aws:iam::" + regionAccount + ":role/ka"),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kinesisanalyticsv2.NewFromConfig(cfg).
				ListApplications(ctx, &kinesisanalyticsv2.ListApplicationsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.ApplicationSummaries,
				func(a kav2types.ApplicationSummary) *string { return a.ApplicationName },
			), nil
		},
	}
}

func sagemakerCase() regionCase {
	return regionCase{
		name: "sagemaker",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := sagemaker.NewFromConfig(cfg).CreateExperiment(ctx, &sagemaker.CreateExperimentInput{
				ExperimentName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := sagemaker.NewFromConfig(cfg).ListExperiments(ctx, &sagemaker.ListExperimentsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.ExperimentSummaries))
			for _, e := range out.ExperimentSummaries {
				names = append(names, aws.ToString(e.ExperimentName))
			}

			return names, nil
		},
	}
}

func xrayCase() regionCase {
	return regionCase{
		name: "xray",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := xray.NewFromConfig(cfg).CreateGroup(ctx, &xray.CreateGroupInput{
				GroupName: aws.String(name), FilterExpression: aws.String("service(\"a\")"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := xray.NewFromConfig(cfg).GetGroups(ctx, &xray.GetGroupsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Groups))
			for _, g := range out.Groups {
				names = append(names, aws.ToString(g.GroupName))
			}

			return names, nil
		},
	}
}

func transferServerID(ctx context.Context, c *transfer.Client) (string, error) {
	out, err := c.ListServers(ctx, &transfer.ListServersInput{})
	if err != nil {
		return "", err
	}

	if len(out.Servers) > 0 {
		return aws.ToString(out.Servers[0].ServerId), nil
	}

	created, err := c.CreateServer(ctx, &transfer.CreateServerInput{})
	if err != nil {
		return "", err
	}

	return aws.ToString(created.ServerId), nil
}

func transferCase() regionCase {
	return regionCase{
		name: "transfer",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			c := transfer.NewFromConfig(cfg)

			id, err := transferServerID(ctx, c)
			if err != nil {
				return err
			}

			_, err = c.CreateUser(ctx, &transfer.CreateUserInput{
				ServerId: aws.String(id), UserName: aws.String(name),
				Role: aws.String("arn:aws:iam::" + regionAccount + ":role/tr"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			c := transfer.NewFromConfig(cfg)

			srv, err := c.ListServers(ctx, &transfer.ListServersInput{})
			if err != nil || len(srv.Servers) == 0 {
				return nil, err
			}

			out, err := c.ListUsers(ctx, &transfer.ListUsersInput{ServerId: srv.Servers[0].ServerId})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Users))
			for _, u := range out.Users {
				names = append(names, aws.ToString(u.UserName))
			}

			return names, nil
		},
	}
}

func route53resolverCase() regionCase {
	return regionCase{
		name: "route53resolver",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := route53resolver.NewFromConfig(cfg).CreateFirewallDomainList(ctx,
				&route53resolver.CreateFirewallDomainListInput{
					Name:             aws.String(name),
					CreatorRequestId: aws.String(name + "-req"),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := route53resolver.NewFromConfig(cfg).ListFirewallDomainLists(ctx,
				&route53resolver.ListFirewallDomainListsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.FirewallDomainLists))
			for _, d := range out.FirewallDomainLists {
				names = append(names, aws.ToString(d.Name))
			}

			return names, nil
		},
	}
}

func vpclatticeCase() regionCase {
	return regionCase{
		name: "vpclattice",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := vpclattice.NewFromConfig(cfg).CreateServiceNetwork(ctx,
				&vpclattice.CreateServiceNetworkInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := vpclattice.NewFromConfig(cfg).ListServiceNetworks(ctx, &vpclattice.ListServiceNetworksInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Items))
			for _, s := range out.Items {
				names = append(names, aws.ToString(s.Name))
			}

			return names, nil
		},
	}
}

func applicationautoscalingCase() regionCase {
	return regionCase{
		name: "applicationautoscaling",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := dynamodb.NewFromConfig(cfg).CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName:   aws.String(name),
				BillingMode: dynamodbtypes.BillingModeProvisioned,
				ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5),
				},
				AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
					{AttributeName: aws.String("k"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
				},
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("k"), KeyType: dynamodbtypes.KeyTypeHash},
				},
			})
			if err != nil {
				return err
			}

			_, err = applicationautoscaling.NewFromConfig(cfg).RegisterScalableTarget(ctx,
				&applicationautoscaling.RegisterScalableTargetInput{
					ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
					ResourceId:        aws.String("table/" + name),
					ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
					MinCapacity:       aws.Int32(1),
					MaxCapacity:       aws.Int32(10),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := applicationautoscaling.NewFromConfig(cfg).DescribeScalableTargets(ctx,
				&applicationautoscaling.DescribeScalableTargetsInput{
					ServiceNamespace: aastypes.ServiceNamespaceDynamodb,
				})
			if err != nil {
				return nil, err
			}

			return tail(
				strs(out.ScalableTargets, func(s aastypes.ScalableTarget) *string { return s.ResourceId }),
				"/",
			), nil
		},
	}
}

func awsconfigCase() regionCase {
	return regionCase{
		name: "awsconfig",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := configservice.NewFromConfig(cfg).PutConfigRule(ctx, &configservice.PutConfigRuleInput{
				ConfigRule: &configtypes.ConfigRule{
					ConfigRuleName: aws.String(name),
					Source: &configtypes.Source{
						Owner:            configtypes.OwnerAws,
						SourceIdentifier: aws.String("S3_BUCKET_VERSIONING_ENABLED"),
					},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := configservice.NewFromConfig(cfg).
				DescribeConfigRules(ctx, &configservice.DescribeConfigRulesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.ConfigRules, func(r configtypes.ConfigRule) *string { return r.ConfigRuleName }), nil
		},
	}
}

func cloudtrailCase() regionCase {
	return regionCase{
		name: "cloudtrail",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			bucket := "trail-" + cfg.Region + "-" + name

			_, err := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }).
				CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucket),
					CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
						LocationConstraint: s3types.BucketLocationConstraint(cfg.Region),
					},
				})
			if err != nil {
				return err
			}

			_, err = cloudtrail.NewFromConfig(cfg).CreateTrail(ctx, &cloudtrail.CreateTrailInput{
				Name: aws.String(name), S3BucketName: aws.String(bucket),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cloudtrail.NewFromConfig(cfg).DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{
				IncludeShadowTrails: aws.Bool(false),
			})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.TrailList))
			for _, t := range out.TrailList {
				names = append(names, aws.ToString(t.Name))
			}

			return names, nil
		},
	}
}

func daxCase() regionCase {
	return regionCase{
		name: "dax",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := dax.NewFromConfig(cfg).CreateParameterGroup(ctx, &dax.CreateParameterGroupInput{
				ParameterGroupName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := dax.NewFromConfig(cfg).DescribeParameterGroups(ctx, &dax.DescribeParameterGroupsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.ParameterGroups))
			for _, p := range out.ParameterGroups {
				names = append(names, aws.ToString(p.ParameterGroupName))
			}

			return names, nil
		},
	}
}

func elasticsearchCase() regionCase {
	return regionCase{
		name: "elasticsearch",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := elasticsearchservice.NewFromConfig(cfg).CreateElasticsearchDomain(ctx,
				&elasticsearchservice.CreateElasticsearchDomainInput{DomainName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := elasticsearchservice.NewFromConfig(cfg).ListDomainNames(ctx,
				&elasticsearchservice.ListDomainNamesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.DomainNames))
			for _, d := range out.DomainNames {
				names = append(names, aws.ToString(d.DomainName))
			}

			return names, nil
		},
	}
}
