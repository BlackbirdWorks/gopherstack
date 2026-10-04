package main

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appmesh"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeconnections"
	ccontypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	cscontypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/databrew"
	databrewtypes "github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/detective"
	"github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice"
	dstypes "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanatypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/aws-sdk-go-v2/service/inspector2"
	inspector2types "github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	"github.com/aws/aws-sdk-go-v2/service/iotdataplane"
)

func servicesAIsolationCases() []regionCase {
	return []regionCase{
		accessanalyzerCase(),
		amplifyCase(),
		appconfigCase(),
		appmeshCase(),
		appstreamCase(),
		bedrockCase(),
		bedrockagentCase(),
		bedrockruntimeCase(),
		cleanroomsCase(),
		codeartifactCase(),
		codeconnectionsCase(),
		codestarconnectionsCase(),
		comprehendCase(),
		databrewCase(),
		datasyncCase(),
		detectiveCase(),
		directconnectCase(),
		directoryserviceCase(),
		dlmCase(),
		dynamodbstreamsCase(),
		ecrpublicCase(),
		glacierCase(),
		grafanaCase(),
		identitystoreCase(),
		inspector2Case(),
		iotanalyticsCase(),
		iotdataplaneCase(),
	}
}

func accessanalyzerCase() regionCase {
	return regionCase{
		name: "accessanalyzer",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := accessanalyzer.NewFromConfig(cfg).CreateAnalyzer(ctx, &accessanalyzer.CreateAnalyzerInput{
				AnalyzerName: aws.String(name), Type: aatypes.TypeAccount,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := accessanalyzer.NewFromConfig(cfg).ListAnalyzers(ctx, &accessanalyzer.ListAnalyzersInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Analyzers))
			for _, a := range out.Analyzers {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func amplifyCase() regionCase {
	return regionCase{
		name: "amplify",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := amplify.NewFromConfig(cfg).CreateApp(ctx, &amplify.CreateAppInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := amplify.NewFromConfig(cfg).ListApps(ctx, &amplify.ListAppsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Apps))
			for _, a := range out.Apps {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func appconfigCase() regionCase {
	return regionCase{
		name: "appconfig",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := appconfig.NewFromConfig(cfg).CreateApplication(ctx, &appconfig.CreateApplicationInput{
				Name: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := appconfig.NewFromConfig(cfg).ListApplications(ctx, &appconfig.ListApplicationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Items))
			for _, a := range out.Items {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func appmeshCase() regionCase {
	return regionCase{
		name: "appmesh",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := appmesh.NewFromConfig(cfg).CreateMesh(ctx, &appmesh.CreateMeshInput{MeshName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := appmesh.NewFromConfig(cfg).ListMeshes(ctx, &appmesh.ListMeshesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Meshes))
			for _, m := range out.Meshes {
				names = append(names, aws.ToString(m.MeshName))
			}

			return names, nil
		},
	}
}

func appstreamCase() regionCase {
	return regionCase{
		name: "appstream",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := appstream.NewFromConfig(cfg).CreateStack(ctx, &appstream.CreateStackInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := appstream.NewFromConfig(cfg).DescribeStacks(ctx, &appstream.DescribeStacksInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Stacks))
			for _, s := range out.Stacks {
				names = append(names, aws.ToString(s.Name))
			}

			return names, nil
		},
	}
}

func bedrockCase() regionCase {
	return regionCase{
		name: "bedrock",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := bedrock.NewFromConfig(cfg).CreateGuardrail(ctx, &bedrock.CreateGuardrailInput{
				Name:                    aws.String(name),
				BlockedInputMessaging:   aws.String("blocked"),
				BlockedOutputsMessaging: aws.String("blocked"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := bedrock.NewFromConfig(cfg).ListGuardrails(ctx, &bedrock.ListGuardrailsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Guardrails))
			for _, g := range out.Guardrails {
				names = append(names, aws.ToString(g.Name))
			}

			return names, nil
		},
	}
}

func bedrockagentCase() regionCase {
	return regionCase{
		name: "bedrockagent",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := bedrockagent.NewFromConfig(cfg).CreateAgent(ctx, &bedrockagent.CreateAgentInput{
				AgentName:            aws.String(name),
				AgentResourceRoleArn: aws.String("arn:aws:iam::" + regionAccount + ":role/agent"),
				FoundationModel:      aws.String("anthropic.claude-v2"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := bedrockagent.NewFromConfig(cfg).ListAgents(ctx, &bedrockagent.ListAgentsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.AgentSummaries))
			for _, a := range out.AgentSummaries {
				names = append(names, aws.ToString(a.AgentName))
			}

			return names, nil
		},
	}
}

func cleanroomsCase() regionCase {
	return regionCase{
		name: "cleanrooms",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := cleanrooms.NewFromConfig(cfg).CreateCollaboration(ctx, &cleanrooms.CreateCollaborationInput{
				Name:                   aws.String(name),
				Description:            aws.String("isolation"),
				CreatorDisplayName:     aws.String("creator"),
				CreatorMemberAbilities: []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
				Members:                []crtypes.MemberSpecification{},
				QueryLogStatus:         crtypes.CollaborationQueryLogStatusEnabled,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := cleanrooms.NewFromConfig(cfg).ListCollaborations(ctx, &cleanrooms.ListCollaborationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.CollaborationList))
			for _, c := range out.CollaborationList {
				names = append(names, aws.ToString(c.Name))
			}

			return names, nil
		},
	}
}

func codeartifactCase() regionCase {
	return regionCase{
		name: "codeartifact",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codeartifact.NewFromConfig(cfg).CreateDomain(ctx, &codeartifact.CreateDomainInput{
				Domain: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codeartifact.NewFromConfig(cfg).ListDomains(ctx, &codeartifact.ListDomainsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Domains))
			for _, d := range out.Domains {
				names = append(names, aws.ToString(d.Name))
			}

			return names, nil
		},
	}
}

func codeconnectionsCase() regionCase {
	return regionCase{
		name: "codeconnections",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codeconnections.NewFromConfig(cfg).CreateConnection(ctx, &codeconnections.CreateConnectionInput{
				ConnectionName: aws.String(name), ProviderType: ccontypes.ProviderTypeGithub,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codeconnections.NewFromConfig(cfg).ListConnections(ctx, &codeconnections.ListConnectionsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Connections))
			for _, c := range out.Connections {
				names = append(names, aws.ToString(c.ConnectionName))
			}

			return names, nil
		},
	}
}

func codestarconnectionsCase() regionCase {
	return regionCase{
		name: "codestarconnections",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := codestarconnections.NewFromConfig(cfg).
				CreateConnection(ctx, &codestarconnections.CreateConnectionInput{
					ConnectionName: aws.String(name), ProviderType: cscontypes.ProviderTypeGithub,
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := codestarconnections.NewFromConfig(cfg).
				ListConnections(ctx, &codestarconnections.ListConnectionsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Connections))
			for _, c := range out.Connections {
				names = append(names, aws.ToString(c.ConnectionName))
			}

			return names, nil
		},
	}
}

func comprehendCase() regionCase {
	return regionCase{
		name: "comprehend",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := comprehend.NewFromConfig(cfg).CreateFlywheel(ctx, &comprehend.CreateFlywheelInput{
				FlywheelName:      aws.String(name),
				DataAccessRoleArn: aws.String("arn:aws:iam::" + regionAccount + ":role/comprehend"),
				DataLakeS3Uri:     aws.String("s3://lake/" + name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := comprehend.NewFromConfig(cfg).ListFlywheels(ctx, &comprehend.ListFlywheelsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.FlywheelSummaryList))
			for _, f := range out.FlywheelSummaryList {
				arn := aws.ToString(f.FlywheelArn)
				names = append(names, arn[strings.LastIndexAny(arn, "/:")+1:])
			}

			return names, nil
		},
	}
}

func databrewCase() regionCase {
	return regionCase{
		name: "databrew",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := databrew.NewFromConfig(cfg).CreateDataset(ctx, &databrew.CreateDatasetInput{
				Name: aws.String(name),
				Input: &databrewtypes.Input{S3InputDefinition: &databrewtypes.S3Location{
					Bucket: aws.String("bucket"), Key: aws.String(name),
				}},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := databrew.NewFromConfig(cfg).ListDatasets(ctx, &databrew.ListDatasetsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Datasets))
			for _, d := range out.Datasets {
				names = append(names, aws.ToString(d.Name))
			}

			return names, nil
		},
	}
}

func datasyncCase() regionCase {
	return regionCase{
		name: "datasync",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := datasync.NewFromConfig(cfg).CreateAgent(ctx, &datasync.CreateAgentInput{
				ActivationKey: aws.String("AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"), AgentName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := datasync.NewFromConfig(cfg).ListAgents(ctx, &datasync.ListAgentsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Agents))
			for _, a := range out.Agents {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func detectiveCase() regionCase {
	return regionCase{
		name: "detective",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			client := detective.NewFromConfig(cfg)

			graph, err := client.CreateGraph(ctx, &detective.CreateGraphInput{})
			if err != nil {
				return err
			}

			_, err = client.TagResource(ctx, &detective.TagResourceInput{
				ResourceArn: graph.GraphArn, Tags: map[string]string{name: "1"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			client := detective.NewFromConfig(cfg)

			out, err := client.ListGraphs(ctx, &detective.ListGraphsInput{})
			if err != nil {
				return nil, err
			}

			var names []string

			for _, g := range out.GraphList {
				tags, tagErr := client.ListTagsForResource(ctx, &detective.ListTagsForResourceInput{ResourceArn: g.Arn})
				if tagErr != nil {
					return nil, tagErr
				}

				for k := range tags.Tags {
					names = append(names, k)
				}
			}

			return names, nil
		},
	}
}

func directconnectCase() regionCase {
	return regionCase{
		name: "directconnect",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := directconnect.NewFromConfig(cfg).CreateConnection(ctx, &directconnect.CreateConnectionInput{
				ConnectionName: aws.String(name), Bandwidth: aws.String("1Gbps"), Location: aws.String("EqDC2"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := directconnect.NewFromConfig(cfg).
				DescribeConnections(ctx, &directconnect.DescribeConnectionsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Connections))
			for _, c := range out.Connections {
				names = append(names, aws.ToString(c.ConnectionName))
			}

			return names, nil
		},
	}
}

func directoryserviceCase() regionCase {
	return regionCase{
		name: "directoryservice",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := directoryservice.NewFromConfig(cfg).CreateDirectory(ctx, &directoryservice.CreateDirectoryInput{
				Name: aws.String(
					name + ".example.com",
				),
				Password: aws.String("Passw0rd!Passw0rd"),
				Size:     dstypes.DirectorySizeSmall,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := directoryservice.NewFromConfig(cfg).
				DescribeDirectories(ctx, &directoryservice.DescribeDirectoriesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.DirectoryDescriptions))
			for _, d := range out.DirectoryDescriptions {
				names = append(names, strings.TrimSuffix(aws.ToString(d.Name), ".example.com"))
			}

			return names, nil
		},
	}
}

func dlmCase() regionCase {
	return regionCase{
		name: "dlm",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := dlm.NewFromConfig(cfg).CreateLifecyclePolicy(ctx, &dlm.CreateLifecyclePolicyInput{
				Description:      aws.String(name),
				ExecutionRoleArn: aws.String("arn:aws:iam::" + regionAccount + ":role/dlm"),
				State:            dlmtypes.SettablePolicyStateValuesEnabled,
				PolicyDetails: &dlmtypes.PolicyDetails{
					ResourceTypes: []dlmtypes.ResourceTypeValues{dlmtypes.ResourceTypeValuesVolume},
					TargetTags:    []dlmtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
					Schedules: []dlmtypes.Schedule{
						{
							Name: aws.String("daily"),
							CreateRule: &dlmtypes.CreateRule{
								Interval:     aws.Int32(24),
								IntervalUnit: dlmtypes.IntervalUnitValuesHours,
							},
							RetainRule: &dlmtypes.RetainRule{Count: aws.Int32(3)},
						},
					},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := dlm.NewFromConfig(cfg).GetLifecyclePolicies(ctx, &dlm.GetLifecyclePoliciesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Policies))
			for _, p := range out.Policies {
				names = append(names, aws.ToString(p.Description))
			}

			return names, nil
		},
	}
}

func ecrpublicCase() regionCase {
	return regionCase{
		name:   "ecrpublic",
		global: true,
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ecrpublic.NewFromConfig(cfg).CreateRepository(ctx, &ecrpublic.CreateRepositoryInput{
				RepositoryName: aws.String(name),
			})

			return err
		},
	}
}

func glacierCase() regionCase {
	return regionCase{
		name: "glacier",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := glacier.NewFromConfig(cfg).CreateVault(ctx, &glacier.CreateVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := glacier.NewFromConfig(cfg).ListVaults(ctx, &glacier.ListVaultsInput{AccountId: aws.String("-")})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.VaultList))
			for _, v := range out.VaultList {
				names = append(names, aws.ToString(v.VaultName))
			}

			return names, nil
		},
	}
}

func grafanaCase() regionCase {
	return regionCase{
		name: "grafana",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := grafana.NewFromConfig(cfg).CreateWorkspace(ctx, &grafana.CreateWorkspaceInput{
				WorkspaceName:     aws.String(name),
				AccountAccessType: grafanatypes.AccountAccessTypeCurrentAccount,
				AuthenticationProviders: []grafanatypes.AuthenticationProviderTypes{
					grafanatypes.AuthenticationProviderTypesSaml,
				},
				PermissionType: grafanatypes.PermissionTypeServiceManaged,
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := grafana.NewFromConfig(cfg).ListWorkspaces(ctx, &grafana.ListWorkspacesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Workspaces))
			for _, w := range out.Workspaces {
				names = append(names, aws.ToString(w.Name))
			}

			return names, nil
		},
	}
}

func identitystoreCase() regionCase {
	return regionCase{
		name: "identitystore",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := identitystore.NewFromConfig(cfg).CreateUser(ctx, &identitystore.CreateUserInput{
				IdentityStoreId: aws.String("d-1234567890"), UserName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := identitystore.NewFromConfig(cfg).ListUsers(ctx, &identitystore.ListUsersInput{
				IdentityStoreId: aws.String("d-1234567890"),
			})
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

func inspector2Case() regionCase {
	return regionCase{
		name: "inspector2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := inspector2.NewFromConfig(cfg).CreateFilter(ctx, &inspector2.CreateFilterInput{
				Name: aws.String(name), Action: inspector2types.FilterActionNone,
				FilterCriteria: &inspector2types.FilterCriteria{},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := inspector2.NewFromConfig(cfg).ListFilters(ctx, &inspector2.ListFiltersInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Filters))
			for _, f := range out.Filters {
				names = append(names, aws.ToString(f.Name))
			}

			return names, nil
		},
	}
}

func iotanalyticsCase() regionCase {
	return regionCase{
		name: "iotanalytics",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := iotanalytics.NewFromConfig(cfg).CreateChannel(ctx, &iotanalytics.CreateChannelInput{
				ChannelName: aws.String(strings.ReplaceAll(name, "-", "_")),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := iotanalytics.NewFromConfig(cfg).ListChannels(ctx, &iotanalytics.ListChannelsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.ChannelSummaries))
			for _, c := range out.ChannelSummaries {
				names = append(names, strings.ReplaceAll(aws.ToString(c.ChannelName), "_", "-"))
			}

			return names, nil
		},
	}
}

func dynamodbstreamsCase() regionCase {
	return regionCase{
		name: "dynamodbstreams",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := dynamodb.NewFromConfig(cfg).CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName:   aws.String(name),
				BillingMode: dynamodbtypes.BillingModePayPerRequest,
				AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
					{AttributeName: aws.String("k"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
				},
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("k"), KeyType: dynamodbtypes.KeyTypeHash},
				},
				StreamSpecification: &dynamodbtypes.StreamSpecification{
					StreamEnabled: aws.Bool(true), StreamViewType: dynamodbtypes.StreamViewTypeNewAndOldImages,
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := dynamodbstreams.NewFromConfig(cfg).ListStreams(ctx, &dynamodbstreams.ListStreamsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Streams))
			for _, s := range out.Streams {
				names = append(names, aws.ToString(s.TableName))
			}

			return names, nil
		},
	}
}

func bedrockruntimeCase() regionCase {
	return regionCase{
		name: "bedrockruntime",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := bedrockruntime.NewFromConfig(cfg).StartAsyncInvoke(ctx, &bedrockruntime.StartAsyncInvokeInput{
				ModelId:            aws.String("amazon.nova-pro-v1:0"),
				ModelInput:         document.NewLazyDocument(map[string]any{"prompt": "hi"}),
				ClientRequestToken: aws.String(name),
				OutputDataConfig: &brtypes.AsyncInvokeOutputDataConfigMemberS3OutputDataConfig{
					Value: brtypes.AsyncInvokeS3OutputDataConfig{S3Uri: aws.String("s3://bucket/out/")},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := bedrockruntime.NewFromConfig(cfg).ListAsyncInvokes(ctx, &bedrockruntime.ListAsyncInvokesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.AsyncInvokeSummaries))
			for _, a := range out.AsyncInvokeSummaries {
				names = append(names, aws.ToString(a.ClientRequestToken))
			}

			return names, nil
		},
	}
}

func iotdataplaneCase() regionCase {
	return regionCase{
		name: "iotdataplane",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := iotdataplane.NewFromConfig(cfg).UpdateThingShadow(ctx, &iotdataplane.UpdateThingShadowInput{
				ThingName:  aws.String("mr-thing"),
				ShadowName: aws.String(name),
				Payload:    []byte(`{"state":{"desired":{"on":true}}}`),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := iotdataplane.NewFromConfig(cfg).
				ListNamedShadowsForThing(ctx, &iotdataplane.ListNamedShadowsForThingInput{
					ThingName: aws.String("mr-thing"),
				})
			if err != nil {
				return nil, err
			}

			return out.Results, nil
		},
	}
}
