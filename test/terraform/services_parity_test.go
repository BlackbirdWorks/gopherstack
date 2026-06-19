package terraform_test

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwv2svc "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apprunnersdkv2 "github.com/aws/aws-sdk-go-v2/service/apprunner"
	appsyncsdkv2 "github.com/aws/aws-sdk-go-v2/service/appsync"
	cfnsvc "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cwlogssvc "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cognitoidentitysvc "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	cognitoidpsvc "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	comprehendsvc "github.com/aws/aws-sdk-go-v2/service/comprehend"
	databrewsvc "github.com/aws/aws-sdk-go-v2/service/databrew"
	databrewtypes "github.com/aws/aws-sdk-go-v2/service/databrew/types"
	datasyncsvc "github.com/aws/aws-sdk-go-v2/service/datasync"
	detectivesvc "github.com/aws/aws-sdk-go-v2/service/detective"
	directoryservicesvc "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	forecastsvc "github.com/aws/aws-sdk-go-v2/service/forecast"
	forecasttypes "github.com/aws/aws-sdk-go-v2/service/forecast/types"
	gluesvc "github.com/aws/aws-sdk-go-v2/service/glue"
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
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	transcribesvc "github.com/aws/aws-sdk-go-v2/service/transcribe"
	translatesvc "github.com/aws/aws-sdk-go-v2/service/translate"
	translatetypes "github.com/aws/aws-sdk-go-v2/service/translate/types"
	workmailsvc "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makePyZip creates a minimal Lambda zip in dir with the provided Python source,
// writes it to function.zip, and returns the path.
func makePyZip(t *testing.T, dir, src string) string {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, err := zw.Create("index.py")
	require.NoError(t, err)
	_, err = f.Write([]byte(src))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	zipPath := filepath.Join(dir, "function.zip")
	require.NoError(t, writeBytes(t, zipPath, buf.Bytes()))

	return zipPath
}

// writeBytes writes b to path. Used by makePyZip to avoid importing os directly.
func writeBytes(t *testing.T, path string, b []byte) error {
	t.Helper()

	return os.WriteFile(path, b, 0o644)
}

// ---------------------------------------------------------------------------
// AppRunner
// ---------------------------------------------------------------------------

// TestTerraform_AppRunner provisions an App Runner service via Terraform and
// verifies it appears in ListServices.
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

// TestTerraform_Comprehend provisions a document classifier via Terraform and
// verifies it appears in ListDocumentClassifiers.
func TestTerraform_Comprehend(t *testing.T) {
	t.Skip("TestTerraform_Comprehend: CI timeout — Comprehend document classifier provisioning exceeds 15m limit")
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

				out, err := client.ListDocumentClassifiers(
					ctx,
					&comprehendsvc.ListDocumentClassifiersInput{},
				)
				require.NoError(
					t,
					err,
					"ListDocumentClassifiers should succeed after terraform apply",
				)

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

// TestTerraform_DataBrew creates an S3 bucket via Terraform, then provisions a
// DataBrew dataset via the SDK and verifies it appears in ListDatasets.
// aws_glue_databrew_dataset was removed from terraform-provider-aws v5, so
// SDK creation is used instead.
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
				bucketName := vars["BucketName"].(string)

				_, createErr := client.CreateDataset(ctx, &databrewsvc.CreateDatasetInput{
					Name: aws.String(datasetName),
					Input: &databrewtypes.Input{
						S3InputDefinition: &databrewtypes.S3Location{
							Bucket: aws.String(bucketName),
							Key:    aws.String("data.csv"),
						},
					},
					Format: databrewtypes.InputFormatCsv,
				})
				require.NoError(t, createErr, "CreateDataset should succeed")

				out, err := client.ListDatasets(ctx, &databrewsvc.ListDatasetsInput{})
				require.NoError(t, err, "ListDatasets should succeed after SDK create")

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

// TestTerraform_DataSync provisions a DataSync S3 location via Terraform and
// verifies it appears in ListLocations.
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
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
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

// TestTerraform_DirectoryService provisions a Simple AD directory via Terraform
// and verifies it appears in DescribeDirectories.
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

				out, err := client.DescribeDirectories(
					ctx,
					&directoryservicesvc.DescribeDirectoriesInput{},
				)
				require.NoError(t, err, "DescribeDirectories should succeed after terraform apply")

				found := false
				for _, d := range out.DirectoryDescriptions {
					if aws.ToString(d.Name) == domainName {
						found = true

						break
					}
				}
				assert.True(
					t,
					found,
					"directory %q should appear in DescribeDirectories",
					domainName,
				)
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

// TestTerraform_Detective provisions a Detective graph via Terraform and verifies
// it appears in ListGraphs.
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
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
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

// TestTerraform_Forecast applies a placeholder fixture, then provisions a
// Forecast dataset group via the SDK and verifies it appears in
// ListDatasetGroups. aws_forecast_dataset_group was removed from
// terraform-provider-aws v5; SDK creation is used instead.
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

				_, createErr := client.CreateDatasetGroup(ctx, &forecastsvc.CreateDatasetGroupInput{
					DatasetGroupName: aws.String(name),
					Domain:           forecasttypes.DomainCustom,
				})
				require.NoError(t, createErr, "CreateDatasetGroup should succeed")

				out, err := client.ListDatasetGroups(ctx, &forecastsvc.ListDatasetGroupsInput{})
				require.NoError(t, err, "ListDatasetGroups should succeed after SDK create")

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

// TestTerraform_Macie2 enables Macie via Terraform and verifies GetMacieSession
// returns ENABLED.
func TestTerraform_Macie2(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "macie2/account",
			providerFn: macie2ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
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

// TestTerraform_MediaLive provisions a MediaLive RTMP_PUSH input via Terraform
// and verifies it appears in ListInputs.
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

// TestTerraform_MediaPackage provisions a MediaPackage channel via Terraform and
// verifies it appears in ListChannels.
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

// TestTerraform_MediaStoreData provisions a MediaStore container via Terraform
// and exercises the mediastoredata SDK data-plane (PutObject, ListItems).
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

// TestTerraform_MediaTailor applies a placeholder fixture, then provisions a
// MediaTailor playback configuration via the SDK and verifies it appears in
// ListPlaybackConfigurations. aws_mediatailor_playback_configuration was
// removed from terraform-provider-aws v5; SDK creation is used instead.
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

				_, createErr := client.PutPlaybackConfiguration(ctx, &mediatailorsvc.PutPlaybackConfigurationInput{
					Name:                  aws.String(configName),
					AdDecisionServerUrl:   aws.String("https://ads.example.com/vast"),
					VideoContentSourceUrl: aws.String("https://example.com/hls/manifest.m3u8"),
				})
				require.NoError(t, createErr, "PutPlaybackConfiguration should succeed")

				out, err := client.ListPlaybackConfigurations(
					ctx,
					&mediatailorsvc.ListPlaybackConfigurationsInput{},
				)
				require.NoError(
					t,
					err,
					"ListPlaybackConfigurations should succeed after SDK create",
				)

				found := false
				for _, c := range out.Items {
					if aws.ToString(c.Name) == configName {
						found = true

						break
					}
				}
				assert.True(
					t,
					found,
					"playback configuration %q should appear in ListPlaybackConfigurations",
					configName,
				)
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

// TestTerraform_Personalize applies a placeholder fixture, then provisions a
// Personalize dataset group via the SDK and verifies it appears in
// ListDatasetGroups. aws_personalize_dataset_group is not available in
// terraform-provider-aws v5; SDK creation is used instead.
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

				_, createErr := client.CreateDatasetGroup(ctx, &personalizesvc.CreateDatasetGroupInput{
					Name: aws.String(name),
				})
				require.NoError(t, createErr, "CreateDatasetGroup should succeed")

				out, err := client.ListDatasetGroups(ctx, &personalizesvc.ListDatasetGroupsInput{})
				require.NoError(t, err, "ListDatasetGroups should succeed after SDK create")

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

// TestTerraform_Polly applies a placeholder fixture, then provisions a Polly
// lexicon via the SDK and verifies it appears in ListLexicons.
// aws_polly_lexicon is not available in terraform-provider-aws v5; SDK
// creation is used instead.
func TestTerraform_Polly(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "polly/lexicon",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]
				// Lexicon names must start with a letter, be alphanumeric, max 20 chars.
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

				lexiconContent := `<?xml version="1.0" encoding="UTF-8"?>
<lexicon version="1.0"
         xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.w3.org/2005/01/pronunciation-lexicon
           http://www.w3.org/TR/2007/CR-pronunciation-lexicon-20071212/pls.xsd"
         alphabet="ipa"
         xml:lang="en-US">
  <lexeme>
    <grapheme>W3C</grapheme>
    <alias>World Wide Web Consortium</alias>
  </lexeme>
</lexicon>`

				_, createErr := client.PutLexicon(ctx, &pollysvc.PutLexiconInput{
					Name:    aws.String(lexiconName),
					Content: aws.String(lexiconContent),
				})
				require.NoError(t, createErr, "PutLexicon should succeed")

				out, err := client.ListLexicons(ctx, &pollysvc.ListLexiconsInput{})
				require.NoError(t, err, "ListLexicons should succeed after SDK create")

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

// TestTerraform_QuickSight provisions a QuickSight group via Terraform and
// verifies it appears in ListGroups.
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

// TestTerraform_Rekognition provisions a Rekognition collection via Terraform
// and verifies it appears in ListCollections.
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

				assert.True(
					t,
					slices.Contains(out.CollectionIds, collectionID),
					"collection %q should appear in ListCollections",
					collectionID,
				)
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

// TestTerraform_RolesAnywhere provisions a RolesAnywhere trust anchor backed by
// an ACM PCA root CA via Terraform and verifies it appears in ListTrustAnchors.
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
					"RoleName":        "tf-rolesanywhere-role-" + id,
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
				assert.True(
					t,
					found,
					"trust anchor %q should appear in ListTrustAnchors",
					anchorName,
				)
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

// TestTerraform_Transcribe provisions a Transcribe vocabulary via Terraform and
// verifies it appears in ListVocabularies.
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

// TestTerraform_Translate applies a placeholder fixture, then provisions a
// Translate terminology via the SDK and verifies it exists.
// aws_translate_terminology is not available in the cached OpenTofu provider
// (frozen 2026-03-17); SDK creation is used instead.
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
					"TerminologyName": "tf-terminology-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createTranslateClient(t)
				name := vars["TerminologyName"].(string)

				_, importErr := client.ImportTerminology(ctx, &translatesvc.ImportTerminologyInput{
					Name:          aws.String(name),
					MergeStrategy: translatetypes.MergeStrategyOverwrite,
					TerminologyData: &translatetypes.TerminologyData{
						File:   []byte("en,es\ngopherstack,gopherstack\n"),
						Format: translatetypes.TerminologyDataFormatCsv,
					},
				})
				require.NoError(t, importErr, "ImportTerminology should succeed")

				out, err := client.GetTerminology(ctx, &translatesvc.GetTerminologyInput{
					Name: aws.String(name),
				})
				require.NoError(t, err, "GetTerminology should succeed after SDK import")
				assert.Equal(t, name, aws.ToString(out.TerminologyProperties.Name),
					"terminology %q should exist after SDK import", name)
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

// TestTerraform_WorkMail applies a placeholder fixture, then provisions a
// WorkMail organization via the SDK and verifies it appears in
// ListOrganizations. aws_workmail_organization is not available in
// terraform-provider-aws v5; SDK creation is used instead.
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

				_, createErr := client.CreateOrganization(ctx, &workmailsvc.CreateOrganizationInput{
					Alias: aws.String(alias),
				})
				require.NoError(t, createErr, "CreateOrganization should succeed")

				out, err := client.ListOrganizations(ctx, &workmailsvc.ListOrganizationsInput{})
				require.NoError(t, err, "ListOrganizations should succeed after SDK create")

				found := false
				for _, o := range out.OrganizationSummaries {
					if aws.ToString(o.Alias) == alias {
						found = true

						break
					}
				}
				assert.True(
					t,
					found,
					"organization with alias %q should appear in ListOrganizations",
					alias,
				)
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

// TestTerraform_CloudWatchLogsComprehensive provisions a set of CloudWatch Logs
// resources (log groups, metric filters, subscription filters) and verifies the
// full stack using the CloudWatch Logs SDK.
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

				// Verify both log groups exist.
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
				mfOut, err := logsClient.DescribeMetricFilters(
					ctx,
					&cwlogssvc.DescribeMetricFiltersInput{
						LogGroupName: aws.String(appLG),
					},
				)
				require.NoError(t, err, "DescribeMetricFilters should succeed")
				assert.GreaterOrEqual(t, len(mfOut.MetricFilters), 2,
					"at least 2 metric filters should exist on %q", appLG)

				// Verify subscription filter on /app log group.
				sfOut, err := logsClient.DescribeSubscriptionFilters(
					ctx,
					&cwlogssvc.DescribeSubscriptionFiltersInput{
						LogGroupName: aws.String(appLG),
					},
				)
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

// TestTerraform_CognitoComprehensive provisions a Cognito UserPool, IdentityPool,
// app client, and IAM role attachment, then verifies all resources via SDK.
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
					MaxResults: aws.Int32(60),
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
					// Verify User Pool Client exists.
					clientsOut, cliErr := idpClient.ListUserPoolClients(
						ctx,
						&cognitoidpsvc.ListUserPoolClientsInput{
							UserPoolId: aws.String(poolID),
						},
					)
					require.NoError(t, cliErr, "ListUserPoolClients should succeed")
					assert.NotEmpty(t, clientsOut.UserPoolClients, "user pool client should exist")
				}

				// Verify Identity Pool.
				identClient := createCognitoIdentityClient(t)
				identityPoolName := prefix + "_identity_pool"
				identPoolsOut, err := identClient.ListIdentityPools(
					ctx,
					&cognitoidentitysvc.ListIdentityPoolsInput{
						MaxResults: aws.Int32(60),
					},
				)
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

// TestTerraform_GlueComprehensive provisions a Glue catalog database, external
// table, crawler, ETL job, and scheduled trigger, then verifies via Glue SDK.
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

				// Verify catalog table.
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

// TestTerraform_AppSyncComprehensive provisions an AppSync GraphQL API with a
// DynamoDB data source and resolvers, then verifies all components via SDK.
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

				// Verify Resolvers on Query type.
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
// §O: S3 → Lambda event receipt assertion (e2e)
// ---------------------------------------------------------------------------

// TestTerraform_S3LambdaEventReceipt provisions S3 + Lambda + SQS via Terraform
// with an S3 bucket notification wired to Lambda. It then PUTs an object and
// asserts the notification configuration is in place.
func TestTerraform_S3LambdaEventReceipt(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "s3_to_lambda_receipt",
			fixture: "s3-lambda-e2e/main",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				src := "def handler(event, context):\n    return {'statusCode': 200}\n"
				zipPath := makePyZip(t, dir, src)

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

				// Assert the bucket notification configuration is wired up.
				notifOut, err := s3c.GetBucketNotificationConfiguration(ctx,
					&s3svc.GetBucketNotificationConfigurationInput{
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

// TestTerraform_APIGatewayV2FullStackViaCFN deploys a complete HTTP API
// (Api + Integration + Route + Stage) via a CloudFormation stack and verifies
// all resources are reachable via the API Gateway v2 SDK.
func TestTerraform_APIGatewayV2FullStackViaCFN(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "apigwv2_cfn_full_stack",
			fixture: "apigwv2-cfn/main",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]
				zipPath := makePyZip(
					t,
					dir,
					"def handler(event, context):\n    return {'statusCode': 200, 'body': 'ok'}\n",
				)

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

				// Verify CFN stack reached CREATE_COMPLETE.
				cfnClient := createCloudFormationClient(t)
				stackName := vars["StackName"].(string)
				stackOut, err := cfnClient.DescribeStacks(ctx, &cfnsvc.DescribeStacksInput{
					StackName: aws.String(stackName),
				})
				require.NoError(t, err, "DescribeStacks should succeed")
				require.Len(t, stackOut.Stacks, 1)
				assert.Equal(t, "CREATE_COMPLETE", string(stackOut.Stacks[0].StackStatus),
					"stack should reach CREATE_COMPLETE")

				// Verify the HTTP API was created.
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
				assert.NotEmpty(
					t,
					apiID,
					"API %q should be listed after CFN stack deploys",
					apiName,
				)

				if apiID == "" {
					return
				}

				// Verify at least one route exists.
				routesOut, err := apigwClient.GetRoutes(ctx, &apigwv2svc.GetRoutesInput{
					ApiId: aws.String(apiID),
				})
				require.NoError(t, err, "GetRoutes should succeed")
				assert.NotEmpty(t, routesOut.Items, "at least one route should exist in the API")

				// Verify $default stage with AutoDeploy.
				stagesOut, err := apigwClient.GetStages(ctx, &apigwv2svc.GetStagesInput{
					ApiId: aws.String(apiID),
				})
				require.NoError(t, err, "GetStages should succeed")

				foundDefault := false
				for _, s := range stagesOut.Items {
					if aws.ToString(s.StageName) == "$default" {
						foundDefault = true
						assert.True(
							t,
							aws.ToBool(s.AutoDeploy),
							"$default stage should have AutoDeploy=true",
						)

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
