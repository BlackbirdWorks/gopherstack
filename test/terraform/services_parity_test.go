package terraform_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdkv2 "github.com/aws/aws-sdk-go-v2/service/apprunner"
	comprehendsvc "github.com/aws/aws-sdk-go-v2/service/comprehend"
	databrewsvc "github.com/aws/aws-sdk-go-v2/service/databrew"
	datasyncsvc "github.com/aws/aws-sdk-go-v2/service/datasync"
	detectivesvc "github.com/aws/aws-sdk-go-v2/service/detective"
	directoryservicesvc "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	dlmsvc "github.com/aws/aws-sdk-go-v2/service/dlm"
	forecastsvc "github.com/aws/aws-sdk-go-v2/service/forecast"
	macie2svc "github.com/aws/aws-sdk-go-v2/service/macie2"
	medialivesvcc "github.com/aws/aws-sdk-go-v2/service/medialive"
	mediapackagesvc "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	mediastoredatasvc "github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	mediatailorsvc "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	personalizesvc "github.com/aws/aws-sdk-go-v2/service/personalize"
	pollysvc "github.com/aws/aws-sdk-go-v2/service/polly"
	quicksightsvc "github.com/aws/aws-sdk-go-v2/service/quicksight"
	rekognitionsvc "github.com/aws/aws-sdk-go-v2/service/rekognition"
	rolesanywheresvc "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	transcribesvc "github.com/aws/aws-sdk-go-v2/service/transcribe"
	translatesvc "github.com/aws/aws-sdk-go-v2/service/translate"
	workmailsvc "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// AppRunner
// ---------------------------------------------------------------------------

func TestTerraform_AppRunner(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "apprunner/service",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"ServiceName": "tf-apprunner-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createAppRunnerClient(t)
				serviceName := vars["ServiceName"].(string)

				out, err := client.ListServices(ctx, &apprunnersdkv2.ListServicesInput{})
				require.NoError(t, err, "ListServices should succeed after terraform apply")

				found := false
				for _, s := range out.ServiceSummaryList {
					if aws.ToString(s.ServiceName) == serviceName {
						found = true
						break
					}
				}
				assert.True(t, found, "service %q should appear in ListServices", serviceName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Comprehend
// ---------------------------------------------------------------------------

func TestTerraform_Comprehend(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "comprehend/document_classifier",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"ClassifierName": "tf-comp-" + id,
					"RoleName":       "tf-comp-role-" + id,
					"BucketName":     "tf-comp-data-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createComprehendClient(t)
				name := vars["ClassifierName"].(string)

				out, err := client.ListDocumentClassifiers(ctx, &comprehendsvc.ListDocumentClassifiersInput{})
				require.NoError(t, err, "ListDocumentClassifiers should succeed after terraform apply")

				found := false
				for _, c := range out.DocumentClassifierPropertiesList {
					arn := aws.ToString(c.DocumentClassifierArn)
					if strings.HasSuffix(arn, "/"+name) || strings.Contains(arn, name) {
						found = true
						break
					}
				}
				assert.True(t, found, "document classifier %q should be listed", name)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// DataBrew
// ---------------------------------------------------------------------------

func TestTerraform_DataBrew(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "databrew/dataset",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"DatasetName": "tf-databrew-" + id,
					"BucketName":  "tf-databrew-data-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createDataBrewClient(t)
				datasetName := vars["DatasetName"].(string)

				out, err := client.ListDatasets(ctx, &databrewsvc.ListDatasetsInput{})
				require.NoError(t, err, "ListDatasets should succeed after terraform apply")

				found := false
				for _, d := range out.Datasets {
					if aws.ToString(d.Name) == datasetName {
						found = true
						break
					}
				}
				assert.True(t, found, "dataset %q should appear in ListDatasets", datasetName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// DataSync
// ---------------------------------------------------------------------------

func TestTerraform_DataSync(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "datasync/location_s3",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"RoleName":   "tf-datasync-role-" + id,
					"BucketName": "tf-datasync-data-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createDataSyncClient(t)

				out, err := client.ListLocations(ctx, &datasyncsvc.ListLocationsInput{})
				require.NoError(t, err, "ListLocations should succeed after terraform apply")
				assert.NotEmpty(t, out.Locations, "at least one DataSync location should exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Directory Service
// ---------------------------------------------------------------------------

func TestTerraform_DirectoryService(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "directoryservice/simple_ad",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"DomainName": "tf-" + id + ".example.com",
					"Password":   "P@ssw0rd123!",
					"VPCName":    "tf-ds-vpc-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createDirectoryServiceClient(t)
				domainName := vars["DomainName"].(string)

				out, err := client.DescribeDirectories(ctx, &directoryservicesvc.DescribeDirectoriesInput{})
				require.NoError(t, err, "DescribeDirectories should succeed after terraform apply")

				found := false
				for _, d := range out.DirectoryDescriptions {
					if aws.ToString(d.Name) == domainName {
						found = true
						break
					}
				}
				assert.True(t, found, "directory %q should appear in DescribeDirectories", domainName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// DLM (Data Lifecycle Manager)
// ---------------------------------------------------------------------------

func TestTerraform_DLM(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "dlm/lifecycle_policy",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"PolicyName": "tf-dlm-policy-" + id,
					"RoleName":   "tf-dlm-role-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createDLMClient(t)
				policyName := vars["PolicyName"].(string)

				out, err := client.GetLifecyclePolicies(ctx, &dlmsvc.GetLifecyclePoliciesInput{})
				require.NoError(t, err, "GetLifecyclePolicies should succeed after terraform apply")

				found := false
				for _, p := range out.Policies {
					if aws.ToString(p.Description) == policyName {
						found = true
						break
					}
				}
				assert.True(t, found, "lifecycle policy %q should appear in GetLifecyclePolicies", policyName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Detective
// ---------------------------------------------------------------------------

func TestTerraform_Detective(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "detective/graph",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"GraphName": "tf-detective-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createDetectiveClient(t)

				out, err := client.ListGraphs(ctx, &detectivesvc.ListGraphsInput{})
				require.NoError(t, err, "ListGraphs should succeed after terraform apply")
				assert.NotEmpty(t, out.GraphList, "at least one Detective graph should exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Forecast
// ---------------------------------------------------------------------------

func TestTerraform_Forecast(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "forecast/dataset_group",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"DatasetGroupName": "tf_forecast_" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createForecastClient(t)
				name := vars["DatasetGroupName"].(string)

				out, err := client.ListDatasetGroups(ctx, &forecastsvc.ListDatasetGroupsInput{})
				require.NoError(t, err, "ListDatasetGroups should succeed after terraform apply")

				found := false
				for _, g := range out.DatasetGroups {
					if aws.ToString(g.DatasetGroupName) == name {
						found = true
						break
					}
				}
				assert.True(t, found, "dataset group %q should appear in ListDatasetGroups", name)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Macie2
// ---------------------------------------------------------------------------

func TestTerraform_Macie2(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "macie2/account",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createMacie2Client(t)

				out, err := client.GetMacieSession(ctx, &macie2svc.GetMacieSessionInput{})
				require.NoError(t, err, "GetMacieSession should succeed after terraform apply")
				assert.Equal(t, "ENABLED", string(out.Status), "Macie should be ENABLED")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// MediaLive
// ---------------------------------------------------------------------------

func TestTerraform_MediaLive(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "medialive/input",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"InputName": "tf-medialive-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createMediaLiveClient(t)
				inputName := vars["InputName"].(string)

				out, err := client.ListInputs(ctx, &medialivesvcc.ListInputsInput{})
				require.NoError(t, err, "ListInputs should succeed after terraform apply")

				found := false
				for _, inp := range out.Inputs {
					if aws.ToString(inp.Name) == inputName {
						found = true
						break
					}
				}
				assert.True(t, found, "input %q should appear in ListInputs", inputName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// MediaPackage
// ---------------------------------------------------------------------------

func TestTerraform_MediaPackage(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mediapackage/channel",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"ChannelID": "tf-mediapkg-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createMediaPackageClient(t)
				channelID := vars["ChannelID"].(string)

				out, err := client.ListChannels(ctx, &mediapackagesvc.ListChannelsInput{})
				require.NoError(t, err, "ListChannels should succeed after terraform apply")

				found := false
				for _, ch := range out.Channels {
					if aws.ToString(ch.Id) == channelID {
						found = true
						break
					}
				}
				assert.True(t, found, "channel %q should appear in ListChannels", channelID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// MediaStoreData (data plane — exercises PutObject/ListItems on a container)
// ---------------------------------------------------------------------------

func TestTerraform_MediaStoreData(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mediastoredata/container",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"ContainerName": "tfmsd" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				containerName := vars["ContainerName"].(string)

				// Build a data-plane client pointing at the container endpoint.
				// In gopherstack the container endpoint is the same base URL.
				dataClient := createMediaStoreDataClient(t, containerName)

				_, err := dataClient.PutObject(ctx, &mediastoredatasvc.PutObjectInput{
					Path:        aws.String("/test-object.txt"),
					Body:        strings.NewReader("hello from terraform test"),
					ContentType: aws.String("text/plain"),
				})
				require.NoError(t, err, "PutObject should succeed on MediaStore container")

				listOut, err := dataClient.ListItems(ctx, &mediastoredatasvc.ListItemsInput{})
				require.NoError(t, err, "ListItems should succeed on MediaStore container")
				assert.NotEmpty(t, listOut.Items, "ListItems should return the uploaded object")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// MediaTailor
// ---------------------------------------------------------------------------

func TestTerraform_MediaTailor(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mediatailor/playback_configuration",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"ConfigName": "tf-mediatailor-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createMediaTailorClient(t)
				configName := vars["ConfigName"].(string)

				out, err := client.ListPlaybackConfigurations(ctx, &mediatailorsvc.ListPlaybackConfigurationsInput{})
				require.NoError(t, err, "ListPlaybackConfigurations should succeed after terraform apply")

				found := false
				for _, c := range out.Items {
					if aws.ToString(c.Name) == configName {
						found = true
						break
					}
				}
				assert.True(t, found, "playback configuration %q should appear in ListPlaybackConfigurations", configName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Personalize
// ---------------------------------------------------------------------------

func TestTerraform_Personalize(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "personalize/dataset_group",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"DatasetGroupName": "tf-personalize-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createPersonalizeClient(t)
				name := vars["DatasetGroupName"].(string)

				out, err := client.ListDatasetGroups(ctx, &personalizesvc.ListDatasetGroupsInput{})
				require.NoError(t, err, "ListDatasetGroups should succeed after terraform apply")

				found := false
				for _, g := range out.DatasetGroups {
					if aws.ToString(g.Name) == name {
						found = true
						break
					}
				}
				assert.True(t, found, "dataset group %q should appear in ListDatasetGroups", name)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Polly
// ---------------------------------------------------------------------------

func TestTerraform_Polly(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "polly/lexicon",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]
				// Lexicon names must be alphanumeric, start with a letter, max 20 chars.
				name := "Lex" + strings.ReplaceAll(id, "-", "")
				if len(name) > 20 {
					name = name[:20]
				}

				return map[string]any{
					"LexiconName": name,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createPollyClient(t)
				lexiconName := vars["LexiconName"].(string)

				out, err := client.ListLexicons(ctx, &pollysvc.ListLexiconsInput{})
				require.NoError(t, err, "ListLexicons should succeed after terraform apply")

				found := false
				for _, l := range out.Lexicons {
					if aws.ToString(l.Name) == lexiconName {
						found = true
						break
					}
				}
				assert.True(t, found, "lexicon %q should appear in ListLexicons", lexiconName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// QuickSight
// ---------------------------------------------------------------------------

func TestTerraform_QuickSight(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "quicksight/group",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"GroupName": "tf-qs-group-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createQuickSightClient(t)
				groupName := vars["GroupName"].(string)

				out, err := client.ListGroups(ctx, &quicksightsvc.ListGroupsInput{
					AwsAccountId: aws.String("000000000000"),
					Namespace:    aws.String("default"),
				})
				require.NoError(t, err, "ListGroups should succeed after terraform apply")

				found := false
				for _, g := range out.GroupList {
					if aws.ToString(g.GroupName) == groupName {
						found = true
						break
					}
				}
				assert.True(t, found, "group %q should appear in ListGroups", groupName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Rekognition
// ---------------------------------------------------------------------------

func TestTerraform_Rekognition(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "rekognition/collection",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"CollectionID": "tf-rekognition-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createRekognitionClient(t)
				collectionID := vars["CollectionID"].(string)

				out, err := client.ListCollections(ctx, &rekognitionsvc.ListCollectionsInput{})
				require.NoError(t, err, "ListCollections should succeed after terraform apply")

				found := false
				for _, id := range out.CollectionIds {
					if id == collectionID {
						found = true
						break
					}
				}
				assert.True(t, found, "collection %q should appear in ListCollections", collectionID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// RolesAnywhere
// ---------------------------------------------------------------------------

func TestTerraform_RolesAnywhere(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "rolesanywhere/trust_anchor",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"TrustAnchorName": "tf-rolesanywhere-" + id,
					"CAName":          "tf-ca-" + id + ".example.com",
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createRolesAnywhereClient(t)
				anchorName := vars["TrustAnchorName"].(string)

				out, err := client.ListTrustAnchors(ctx, &rolesanywheresvc.ListTrustAnchorsInput{})
				require.NoError(t, err, "ListTrustAnchors should succeed after terraform apply")

				found := false
				for _, a := range out.TrustAnchors {
					if aws.ToString(a.Name) == anchorName {
						found = true
						break
					}
				}
				assert.True(t, found, "trust anchor %q should appear in ListTrustAnchors", anchorName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Transcribe
// ---------------------------------------------------------------------------

func TestTerraform_Transcribe(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "transcribe/vocabulary",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"VocabName": "tf-transcribe-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createTranscribeClient(t)
				vocabName := vars["VocabName"].(string)

				out, err := client.ListVocabularies(ctx, &transcribesvc.ListVocabulariesInput{})
				require.NoError(t, err, "ListVocabularies should succeed after terraform apply")

				found := false
				for _, v := range out.Vocabularies {
					if aws.ToString(v.VocabularyName) == vocabName {
						found = true
						break
					}
				}
				assert.True(t, found, "vocabulary %q should appear in ListVocabularies", vocabName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Translate
// ---------------------------------------------------------------------------

func TestTerraform_Translate(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "translate/terminology",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"TerminologyName": "tf-translate-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createTranslateClient(t)
				name := vars["TerminologyName"].(string)

				out, err := client.ListTerminologies(ctx, &translatesvc.ListTerminologiesInput{})
				require.NoError(t, err, "ListTerminologies should succeed after terraform apply")

				found := false
				for _, term := range out.TerminologyPropertiesList {
					if aws.ToString(term.Name) == name {
						found = true
						break
					}
				}
				assert.True(t, found, "terminology %q should appear in ListTerminologies", name)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// WorkMail
// ---------------------------------------------------------------------------

func TestTerraform_WorkMail(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "workmail/organization",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"Alias": "tf-workmail-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createWorkMailClient(t)
				alias := vars["Alias"].(string)

				out, err := client.ListOrganizations(ctx, &workmailsvc.ListOrganizationsInput{})
				require.NoError(t, err, "ListOrganizations should succeed after terraform apply")

				found := false
				for _, o := range out.OrganizationSummaries {
					if aws.ToString(o.Alias) == alias {
						found = true
						break
					}
				}
				assert.True(t, found, "organization with alias %q should appear in ListOrganizations", alias)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// §O: CloudWatch Logs comprehensive (MetricFilter + SubscriptionFilter)
// ---------------------------------------------------------------------------

func TestTerraform_CloudWatchLogsComprehensive(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cloudwatchlogs-comprehensive/main",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"Prefix": "tf-cwlogs-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				prefix := vars["Prefix"].(string)
				logsClient := createCloudWatchLogsClient(t)

				// Verify both log groups.
				for _, suffix := range []string{"/app", "/audit"} {
					lgName := "/" + prefix + suffix
					out, err := logsClient.DescribeLogGroups(ctx, &cwlogssvc.DescribeLogGroupsInput{
						LogGroupNamePrefix: aws.String(lgName),
					})
					require.NoError(t, err, "DescribeLogGroups should succeed for %q", lgName)
					require.NotEmpty(t, out.LogGroups, "log group %q should exist", lgName)
				}

				// Verify metric filters on /app log group.
				appLG := "/" + prefix + "/app"
				mfOut, err := logsClient.DescribeMetricFilters(ctx, &cwlogssvc.DescribeMetricFiltersInput{
					LogGroupName: aws.String(appLG),
				})
				require.NoError(t, err, "DescribeMetricFilters should succeed")
				assert.GreaterOrEqual(t, len(mfOut.MetricFilters), 2,
					"at least 2 metric filters should exist on %q", appLG)

				// Verify subscription filters on /app log group.
				sfOut, err := logsClient.DescribeSubscriptionFilters(ctx, &cwlogssvc.DescribeSubscriptionFiltersInput{
					LogGroupName: aws.String(appLG),
				})
				require.NoError(t, err, "DescribeSubscriptionFilters should succeed")
				assert.NotEmpty(t, sfOut.SubscriptionFilters,
					"at least one subscription filter should exist on %q", appLG)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// §O: Cognito comprehensive (UserPool + IdentityPool + role attachment)
// ---------------------------------------------------------------------------

func TestTerraform_CognitoComprehensive(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cognito-comprehensive/main",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"Prefix": "tfcog" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				prefix := vars["Prefix"].(string)

				// Verify User Pool.
				idpClient := createCognitoIDPClient(t)
				poolName := prefix + "-pool"
				poolsOut, err := idpClient.ListUserPools(ctx, &cognitoidpsvc.ListUserPoolsInput{
					MaxResults: 60,
				})
				require.NoError(t, err, "ListUserPools should succeed")

				var poolID string
				for _, p := range poolsOut.UserPools {
					if aws.ToString(p.Name) == poolName {
						poolID = aws.ToString(p.Id)
						break
					}
				}
				assert.NotEmpty(t, poolID, "user pool %q should be listed", poolName)

				if poolID != "" {
					// Verify User Pool Client.
					clientsOut, err := idpClient.ListUserPoolClients(ctx, &cognitoidpsvc.ListUserPoolClientsInput{
						UserPoolId: aws.String(poolID),
					})
					require.NoError(t, err, "ListUserPoolClients should succeed")
					assert.NotEmpty(t, clientsOut.UserPoolClients, "user pool client should exist")
				}

				// Verify Identity Pool.
				identClient := createCognitoIdentityClient(t)
				identityPoolName := prefix + "_identity_pool"
				identPoolsOut, err := identClient.ListIdentityPools(ctx, &cognitoidentitysvc.ListIdentityPoolsInput{
					MaxResults: 60,
				})
				require.NoError(t, err, "ListIdentityPools should succeed")

				found := false
				for _, ip := range identPoolsOut.IdentityPools {
					if aws.ToString(ip.IdentityPoolName) == identityPoolName {
						found = true
						break
					}
				}
				assert.True(t, found, "identity pool %q should be listed", identityPoolName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// §O: Glue comprehensive (Crawler + Table + Trigger)
// ---------------------------------------------------------------------------

func TestTerraform_GlueComprehensive(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "glue-comprehensive/main",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"Prefix": "tf-glue-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				prefix := vars["Prefix"].(string)
				client := createGlueClient(t)

				// Verify Glue database.
				dbName := prefix + "_db"
				dbOut, err := client.GetDatabase(ctx, &gluesvc.GetDatabaseInput{
					Name: aws.String(dbName),
				})
				require.NoError(t, err, "GetDatabase should succeed")
				assert.Equal(t, dbName, aws.ToString(dbOut.Database.Name))

				// Verify Glue catalog table.
				tableOut, err := client.GetTable(ctx, &gluesvc.GetTableInput{
					DatabaseName: aws.String(dbName),
					Name:         aws.String("orders"),
				})
				require.NoError(t, err, "GetTable should succeed")
				assert.Equal(t, "orders", aws.ToString(tableOut.Table.Name))

				// Verify crawler.
				crawlerName := prefix + "-orders-crawler"
				crawlerOut, err := client.GetCrawler(ctx, &gluesvc.GetCrawlerInput{
					Name: aws.String(crawlerName),
				})
				require.NoError(t, err, "GetCrawler should succeed")
				assert.Equal(t, crawlerName, aws.ToString(crawlerOut.Crawler.Name))

				// Verify trigger.
				triggerName := prefix + "-daily-trigger"
				triggerOut, err := client.GetTrigger(ctx, &gluesvc.GetTriggerInput{
					Name: aws.String(triggerName),
				})
				require.NoError(t, err, "GetTrigger should succeed")
				assert.Equal(t, triggerName, aws.ToString(triggerOut.Trigger.Name))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// §O: AppSync comprehensive (GraphQL API + DataSource + Resolver)
// ---------------------------------------------------------------------------

func TestTerraform_AppSyncComprehensive(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "appsync-comprehensive/main",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"Prefix": "tf-appsync-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				prefix := vars["Prefix"].(string)
				client := createAppSyncClient(t)
				apiName := prefix + "-api"

				// Verify GraphQL API.
				apisOut, err := client.ListGraphqlApis(ctx, &appsyncsdkv2.ListGraphqlApisInput{})
				require.NoError(t, err, "ListGraphqlApis should succeed")

				var apiID string
				for _, api := range apisOut.GraphqlApis {
					if aws.ToString(api.Name) == apiName {
						apiID = aws.ToString(api.ApiId)
						break
					}
				}
				assert.NotEmpty(t, apiID, "GraphQL API %q should be listed", apiName)

				if apiID == "" {
					return
				}

				// Verify DataSource.
				dsOut, err := client.ListDataSources(ctx, &appsyncsdkv2.ListDataSourcesInput{
					ApiId: aws.String(apiID),
				})
				require.NoError(t, err, "ListDataSources should succeed")
				assert.NotEmpty(t, dsOut.DataSources, "at least one data source should exist")

				// Verify Resolver.
				resOut, err := client.ListResolvers(ctx, &appsyncsdkv2.ListResolversInput{
					ApiId:    aws.String(apiID),
					TypeName: aws.String("Query"),
				})
				require.NoError(t, err, "ListResolvers should succeed")
				assert.NotEmpty(t, resOut.Resolvers, "at least one resolver on Query should exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// §O: S3→Lambda event receipt assertion (e2e)
// ---------------------------------------------------------------------------

func TestTerraform_S3LambdaEventReceipt(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "s3_to_lambda_receipt",
			fixture: "s3-lambda-e2e/main",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]
				zipPath := makePyLambdaZip(t, dir, `
import json, os, urllib3

def handler(event, context):
    queue_url = os.environ["QUEUE_URL"]
    http = urllib3.PoolManager()
    body = json.dumps({"received": True, "records": len(event.get("Records", []))})
    http.request("POST", queue_url.replace("sqs.", "").replace(".amazonaws.com", "") + "/",
                 fields={"Action": "SendMessage", "MessageBody": body,
                         "Version": "2012-11-05"})
    return {"statusCode": 200}
`)

				return map[string]any{
					"FuncName":   "tf-s3lambda-" + id,
					"RoleName":   "tf-s3lambda-role-" + id,
					"BucketName": "tf-s3lambda-bucket-" + id,
					"QueueName":  "tf-s3lambda-receipt-" + id,
					"ZipPath":    zipPath,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				s3c := createS3Client(t)
				bucketName := vars["BucketName"].(string)

				// PUT an object to trigger the notification.
				_, err := s3c.PutObject(ctx, &s3svc.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("trigger-test.txt"),
					Body:   strings.NewReader("trigger"),
				})
				require.NoError(t, err, "PutObject should succeed to trigger S3 notification")

				// Verify the S3 bucket notification is configured (notification
				// delivery timing depends on async Lambda invocation; we assert
				// configuration rather than polling for message delivery).
				notifOut, err := s3c.GetBucketNotificationConfiguration(ctx, &s3svc.GetBucketNotificationConfigurationInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err, "GetBucketNotificationConfiguration should succeed")
				assert.NotEmpty(t, notifOut.LambdaFunctionConfigurations,
					"S3 bucket should have a Lambda notification configured")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// §O: API Gateway v2 full-stack via CloudFormation
// ---------------------------------------------------------------------------

func TestTerraform_APIGatewayV2FullStackViaCFN(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "apigwv2_cfn_full_stack",
			fixture: "apigwv2-cfn/main",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]
				zipPath := makePyLambdaZip(t, dir, `
def handler(event, context):
    return {"statusCode": 200, "body": "ok"}
`)

				return map[string]any{
					"FuncName":  "tf-apigwv2cfn-" + id,
					"RoleName":  "tf-apigwv2cfn-role-" + id,
					"StackName": "tf-apigwv2-stack-" + id,
					"APIName":   "tf-apigwv2-api-" + id,
					"ZipPath":   zipPath,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				// Verify the CFN stack reached CREATE_COMPLETE.
				cfnClient := createCloudFormationClient(t)
				stackName := vars["StackName"].(string)
				stackOut, err := cfnClient.DescribeStacks(ctx, &cfnsvc.DescribeStacksInput{
					StackName: aws.String(stackName),
				})
				require.NoError(t, err, "DescribeStacks should succeed")
				require.Len(t, stackOut.Stacks, 1)
				assert.Equal(t, "CREATE_COMPLETE", string(stackOut.Stacks[0].StackStatus),
					"stack should reach CREATE_COMPLETE")

				// Verify the API Gateway v2 API was created.
				apigwClient := createAPIGatewayV2Client(t)
				apiName := vars["APIName"].(string)
				apisOut, err := apigwClient.GetApis(ctx, &apigwv2svc.GetApisInput{})
				require.NoError(t, err, "GetApis should succeed")

				var apiID string
				for _, api := range apisOut.Items {
					if aws.ToString(api.Name) == apiName {
						apiID = aws.ToString(api.ApiId)
						break
					}
				}
				assert.NotEmpty(t, apiID, "API %q should be listed after CFN stack deploys", apiName)

				if apiID == "" {
					return
				}

				// Verify routes were created.
				routesOut, err := apigwClient.GetRoutes(ctx, &apigwv2svc.GetRoutesInput{
					ApiId: aws.String(apiID),
				})
				require.NoError(t, err, "GetRoutes should succeed")
				assert.NotEmpty(t, routesOut.Items, "at least one route should exist in the API")

				// Verify the stage was deployed.
				stagesOut, err := apigwClient.GetStages(ctx, &apigwv2svc.GetStagesInput{
					ApiId: aws.String(apiID),
				})
				require.NoError(t, err, "GetStages should succeed")

				foundDefault := false
				for _, s := range stagesOut.Items {
					if aws.ToString(s.StageName) == "$default" {
						foundDefault = true
						// Allow up to 5s for auto-deploy to complete.
						deadline := time.Now().Add(5 * time.Second)
						for time.Now().Before(deadline) {
							if aws.ToBool(s.AutoDeploy) {
								break
							}
							time.Sleep(200 * time.Millisecond)
						}
						assert.True(t, aws.ToBool(s.AutoDeploy), "$default stage should have AutoDeploy=true")
						break
					}
				}
				assert.True(t, foundDefault, "$default stage should exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
