package terraform_test

import (
	"archive/zip"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsvc "github.com/aws/aws-sdk-go-v2/service/acm"
	apigwsvc "github.com/aws/aws-sdk-go-v2/service/apigateway"
	cfnsvc "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cwsvc "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	cwlogssvc "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	configsvc "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	elasticachesvc "github.com/aws/aws-sdk-go-v2/service/elasticache"
	ebsvc "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	firehosesvc "github.com/aws/aws-sdk-go-v2/service/firehose"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	kinesissvc "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kmssvc "github.com/aws/aws-sdk-go-v2/service/kms"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	opensearchsvc "github.com/aws/aws-sdk-go-v2/service/opensearch"
	rdssvc "github.com/aws/aws-sdk-go-v2/service/rds"
	redshiftsvc "github.com/aws/aws-sdk-go-v2/service/redshift"
	resourcegroupssvc "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	route53resolversvc "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3controlsvc "github.com/aws/aws-sdk-go-v2/service/s3control"
	schedulersvc "github.com/aws/aws-sdk-go-v2/service/scheduler"
	secretssvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sessvc "github.com/aws/aws-sdk-go-v2/service/ses"
	sfnsvc "github.com/aws/aws-sdk-go-v2/service/sfn"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsvc "github.com/aws/aws-sdk-go-v2/service/ssm"
	swfsvc "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureFS holds the embedded fixtures directory tree.
//
//go:embed fixtures
var fixtureFS embed.FS

// tofuProviderCacheDir is a shared provider cache so all tests only download
// the hashicorp/aws provider once per test run.
//
//nolint:gochecknoglobals // shared provider cache path, read-only after init
var tofuProviderCacheDir = filepath.Join(os.TempDir(), "gopherstack-tofu-provider-cache")

// preInitDirMain and preInitDirRDS are directories initialised by warmProviderCache
// in TestMain. applyTofu hard-links .terraform/ from these into each test's temp dir
// instead of running tofu init, giving each test its own independent .terraform/
// subtree (no file-lock contention on terraform.tfstate) while sharing the large
// provider binary via hard links.
//
//nolint:gochecknoglobals // set once in TestMain, read-only during parallel tests
var (
	preInitDirMain string
	preInitDirRDS  string
)

// tofuBinaryOnce ensures tofu is only downloaded once per test run.
//
//nolint:gochecknoglobals // lazy-init singleton for the tofu binary path
var (
	tofuBinaryOnce sync.Once
	tofuBinaryPath string
	errTofuBinary  error
)

// Sentinel errors for the OpenTofu binary download helper.
var (
	errNoTofuVersions = errors.New("no stable versions found in OpenTofu API response")
	errTofuNotInZip   = errors.New("tofu binary not found in zip archive")
)

// providerBlock returns the OpenTofu required_providers + provider "aws" block
// pointing all service endpoints at addr (e.g. "http://localhost:32768").
func providerBlock(addr string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  # Endpoints are listed alphabetically — keep them sorted when adding new ones.
  endpoints {
    acm            = %[1]q
    apigateway     = %[1]q
    cloudformation = %[1]q
    cloudwatch     = %[1]q
    cloudwatchlogs = %[1]q
    configservice  = %[1]q
    dynamodb       = %[1]q
    ec2            = %[1]q
    elasticache    = %[1]q
    events         = %[1]q
    firehose       = %[1]q
    iam            = %[1]q
    kinesis        = %[1]q
    kms            = %[1]q
    lambda         = %[1]q
    opensearch     = %[1]q
    redshift       = %[1]q
    resourcegroups = %[1]q
    route53        = %[1]q
    route53resolver = %[1]q
    s3             = %[1]q
    s3control      = %[1]q
    scheduler      = %[1]q
    secretsmanager = %[1]q
    ses            = %[1]q
    sfn            = %[1]q
    sns            = %[1]q
    sqs            = %[1]q
    ssm            = %[1]q
    sts            = %[1]q
    swf            = %[1]q
  }
}
`, addr)
}

// rdsProviderBlock returns an OpenTofu provider block that includes the rds endpoint.
func rdsProviderBlock(addr string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    rds = %[1]q
    sts = %[1]q
  }
}
`, addr)
}

// ensureTofuBinary returns the path to the tofu binary, downloading it if
// necessary. The download happens at most once per test run.
func ensureTofuBinary(t *testing.T) string {
	t.Helper()

	tofuBinaryOnce.Do(func() {
		if path, err := exec.LookPath("tofu"); err == nil {
			tofuBinaryPath = path

			return
		}

		tofuBinaryPath, errTofuBinary = downloadTofuBinary(slog.Default())
	})

	if errTofuBinary != nil {
		require.NoError(t, errTofuBinary, "could not obtain tofu binary")
	}

	return tofuBinaryPath
}

// downloadTofuBinary fetches the latest stable OpenTofu release for the current
// platform, extracts the binary to [os.TempDir], and returns its path.
func downloadTofuBinary(logger *slog.Logger) (string, error) {
	ctx := context.Background()

	versionReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://get.opentofu.org/tofu/api.json", nil)
	if err != nil {
		return "", fmt.Errorf("creating OpenTofu version request: %w", err)
	}
	resp, err := http.DefaultClient.Do(versionReq)
	if err != nil {
		return "", fmt.Errorf("fetching OpenTofu version list: %w", err)
	}
	defer resp.Body.Close()

	var api struct {
		Versions []struct {
			ID string `json:"id"`
		} `json:"versions"`
	}
	err = json.NewDecoder(resp.Body).Decode(&api)
	if err != nil {
		return "", fmt.Errorf("decoding OpenTofu version list: %w", err)
	}

	version := latestStableTofuVersion(api.Versions)
	if version == "" {
		return "", errNoTofuVersions
	}
	goos := runtime.GOOS
	goarch := runtime.GOARCH

	downloadURL := fmt.Sprintf(
		"https://github.com/opentofu/opentofu/releases/download/v%s/tofu_%s_%s_%s.zip",
		version, version, goos, goarch,
	)
	logger.Info("downloading OpenTofu", "version", version, "url", downloadURL)

	zipReq, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating OpenTofu download request: %w", err)
	}
	zipResp, err := http.DefaultClient.Do(zipReq)
	if err != nil {
		return "", fmt.Errorf("downloading OpenTofu zip: %w", err)
	}
	defer zipResp.Body.Close()

	zipData, err := io.ReadAll(zipResp.Body)
	if err != nil {
		return "", fmt.Errorf("reading OpenTofu zip: %w", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return "", fmt.Errorf("opening OpenTofu zip: %w", err)
	}

	binaryName := "tofu"
	if goos == "windows" {
		binaryName = "tofu.exe"
	}

	for _, f := range zr.File {
		if f.Name != binaryName {
			continue
		}

		return extractTofuBinary(f, binaryName)
	}

	return "", errTofuNotInZip
}

// extractTofuBinary writes a single [zip.File] to [os.TempDir] with executable
// permissions and returns its path.
func extractTofuBinary(f *zip.File, binaryName string) (string, error) {
	destPath := filepath.Join(os.TempDir(), binaryName)

	rc, err := f.Open()
	if err != nil {
		return "", fmt.Errorf("opening %s in zip: %w", binaryName, err)
	}
	defer rc.Close()

	out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return "", fmt.Errorf("creating tofu binary: %w", err)
	}
	defer out.Close()

	if _, err = io.Copy(out, rc); err != nil {
		return "", fmt.Errorf("writing tofu binary: %w", err)
	}

	return destPath, nil
}

// latestStableTofuVersion returns the first stable (non-pre-release) version ID
// from the OpenTofu API versions list, or an empty string if none is found.
func latestStableTofuVersion(versions []struct {
	ID string `json:"id"`
}) string {
	for _, v := range versions {
		if !strings.Contains(v.ID, "-") {
			return v.ID
		}
	}

	return ""
}

// hardLinkDir recreates the directory tree rooted at src inside dst, creating
// hard links for immutable files (provider binaries) and copies for mutable
// metadata files (*.tfstate). Hard-linking the provider binary (~200 MB) is
// near-instant and gives each test its own independent .terraform/ subtree.
// Mutable .tfstate files are copied so each test has a separate inode and
// writes (e.g. .terraform/terraform.tfstate) never contend across tests.
func hardLinkDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, relErr := filepath.Rel(src, path)
		if relErr != nil {
			return relErr
		}

		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}

		// Copy mutable state files so each test has an independent inode.
		if strings.HasSuffix(path, ".tfstate") {
			return copyFile(path, target, info.Mode())
		}

		return os.Link(path, target)
	})
}

// copyFile copies the content of src to dst with the given file mode.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	if _, err = io.Copy(out, in); err != nil {
		out.Close()

		return err
	}

	if err = out.Sync(); err != nil {
		out.Close()

		return err
	}

	return out.Close()
}

// selectPreInitDir returns the pre-initialised directory whose .terraform/ subtree
// can be reused for the given HCL configuration, or an empty string if none matches.
func selectPreInitDir(hcl string) string {
	switch {
	case strings.HasPrefix(hcl, rdsProviderBlock(endpoint)):
		return preInitDirRDS
	case strings.HasPrefix(hcl, providerBlock(endpoint)):
		return preInitDirMain
	default:
		return ""
	}
}

// reuseOrInit hard-links the pre-initialised .terraform/ subtree for the HCL
// configuration into dir when one is available, falling back to tofu init.
// Extracted from applyTofu to avoid deeply nested conditionals.
func reuseOrInit(t *testing.T, dir, hcl string, run func(bool, ...string) bool) {
	t.Helper()

	initSrc := selectPreInitDir(hcl)
	if initSrc == "" {
		run(true, "init", "-no-color")

		return
	}

	dotTerraform := filepath.Join(dir, ".terraform")
	if err := hardLinkDir(filepath.Join(initSrc, ".terraform"), dotTerraform); err != nil {
		t.Logf("could not hard-link .terraform from pre-init dir: %v; cleaning up and falling back to init", err)
		// Remove any partially-created tree so tofu init starts from a clean slate.
		if rmErr := os.RemoveAll(dotTerraform); rmErr != nil {
			t.Logf("failed to remove partial .terraform dir: %v", rmErr)
		}

		run(true, "init", "-no-color")

		return
	}

	// Copy the lock file (small, ~1 KB) so tofu can verify provider checksums.
	// Treat a missing or unreadable lock file as a hard error: without it tofu
	// may re-resolve providers in a non-deterministic way.
	lockData, readErr := os.ReadFile(filepath.Join(initSrc, ".terraform.lock.hcl"))
	if readErr != nil {
		t.Logf("failed to read .terraform.lock.hcl from pre-init dir: %v; falling back to init", readErr)
		if rmErr := os.RemoveAll(dotTerraform); rmErr != nil {
			t.Logf("failed to remove .terraform dir before fallback: %v", rmErr)
		}

		run(true, "init", "-no-color")

		return
	}

	if writeErr := os.WriteFile(filepath.Join(dir, ".terraform.lock.hcl"), lockData, 0o644); writeErr != nil {
		t.Logf("failed to write .terraform.lock.hcl: %v; falling back to init", writeErr)
		if rmErr := os.RemoveAll(dotTerraform); rmErr != nil {
			t.Logf("failed to remove .terraform dir before fallback: %v", rmErr)
		}

		run(true, "init", "-no-color")
	}
}

// applyTofu writes hcl to a main.tf in dir, runs tofu init (or reuses a
// pre-initialised .terraform/ via hardLinkDir), runs tofu apply, and registers
// a cleanup that destroys all created resources.
func applyTofu(t *testing.T, tofuBin, dir, hcl string) {
	t.Helper()

	cfgPath := dir + "/main.tf"
	require.NoError(t, os.WriteFile(cfgPath, []byte(hcl), 0o644))

	if err := os.MkdirAll(tofuProviderCacheDir, 0o755); err != nil {
		t.Logf("could not create provider cache dir: %v", err)
	}

	env := append(os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+tofuProviderCacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
	)

	run := func(failFatal bool, args ...string) bool {
		t.Helper()

		cmd := exec.Command(tofuBin, args...)
		cmd.Dir = dir
		cmd.Env = env

		out, err := cmd.CombinedOutput()
		t.Logf("tofu %v:\n%s", args, out)

		if err != nil {
			if failFatal {
				require.NoError(t, err, "tofu %v failed", args)
			}

			t.Logf("tofu %v failed (non-fatal): %v", args, err)

			return false
		}

		return true
	}

	// Reuse a pre-initialised .terraform directory when available.
	// hardLinkDir copies the directory tree with hard links so the large
	// provider binary is shared (no extra disk use) while each test keeps
	// its own independent directory entries — eliminating the file-lock
	// contention on .terraform/terraform.tfstate that serialised parallel runs.
	reuseOrInit(t, dir, hcl, run)

	run(true, "apply", "-auto-approve", "-no-color")

	t.Cleanup(func() {
		run(false, "destroy", "-auto-approve", "-no-color")
	})
}

// renderFixture reads fixtures/<name>.tf, renders it as a Go text/template
// with vars, and returns the resulting HCL string. name uses forward-slash
// paths, e.g. "dynamodb/success".
func renderFixture(t *testing.T, name string, vars map[string]any) string {
	t.Helper()

	path := "fixtures/" + name + ".tf"

	content, err := fixtureFS.ReadFile(path)
	require.NoError(t, err, "reading fixture %s", path)

	tmpl, err := template.New(name).Option("missingkey=error").Parse(string(content))
	require.NoError(t, err, "parsing fixture %s", path)

	var buf strings.Builder
	require.NoError(t, tmpl.Execute(&buf, vars), "rendering fixture %s", path)

	return buf.String()
}

// tfTestCase describes one scenario within a per-service Terraform test table.
// Add new entries to a service's []tfTestCase slice to cover additional inputs
// or failure paths without touching the test runner.
type tfTestCase struct {
	providerFn func(addr string) string
	setup      func(t *testing.T, dir string) map[string]any
	verify     func(t *testing.T, ctx context.Context, vars map[string]any)
	name       string
	fixture    string
}

// runTFTest is the common runner shared by all per-service Terraform tests.
// It renders the fixture, applies Terraform, then calls verify (if set).
func runTFTest(t *testing.T, tc tfTestCase) {
	t.Helper()
	dumpContainerLogsOnFailure(t)

	tofuBin := ensureTofuBinary(t)
	ctx := context.Background()
	dir := t.TempDir()

	provFn := tc.providerFn
	if provFn == nil {
		provFn = providerBlock
	}

	vars := tc.setup(t, dir)
	hcl := provFn(endpoint) + renderFixture(t, tc.fixture, vars)
	applyTofu(t, tofuBin, dir, hcl)

	if tc.verify != nil {
		tc.verify(t, ctx, vars)
	}
}

// TestTerraform_DynamoDB provisions a DynamoDB table and verifies key schema.
func TestTerraform_DynamoDB(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "dynamodb/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"TableName": "tf-ddb-" + uuid.NewString()}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createDynamoDBClient(t)
				out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(vars["TableName"].(string)),
				})
				require.NoError(t, err, "DescribeTable should succeed after terraform apply")
				require.NotNil(t, out.Table)
				assert.Equal(t, vars["TableName"].(string), aws.ToString(out.Table.TableName))
				require.Len(t, out.Table.KeySchema, 1)
				assert.Equal(t, "pk", aws.ToString(out.Table.KeySchema[0].AttributeName))
				assert.Equal(t, ddbtypes.KeyTypeHash, out.Table.KeySchema[0].KeyType)
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

// TestTerraform_S3AndSQS provisions an S3 bucket and SQS queue and verifies both.
func TestTerraform_S3AndSQS(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "s3_sqs/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()

				return map[string]any{
					"BucketName": "tf-s3-" + id,
					"QueueName":  "tf-sqs-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				s3Client := createS3Client(t)
				_, err := s3Client.HeadBucket(ctx, &s3svc.HeadBucketInput{
					Bucket: aws.String(vars["BucketName"].(string)),
				})
				require.NoError(t, err, "HeadBucket should succeed after terraform apply")

				sqsClient := createSQSClient(t)
				getURLOut, err := sqsClient.GetQueueUrl(ctx, &sqssvc.GetQueueUrlInput{
					QueueName: aws.String(vars["QueueName"].(string)),
				})
				require.NoError(t, err, "GetQueueUrl should succeed after terraform apply")
				assert.Contains(t, aws.ToString(getURLOut.QueueUrl), vars["QueueName"].(string))
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

// TestTerraform_RDS provisions an RDS DB instance and verifies it exists.
func TestTerraform_RDS(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "rds/success",
			providerFn: rdsProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Identifier": "tf-rds-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createRDSClient(t)
				out, err := client.DescribeDBInstances(ctx, &rdssvc.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String(vars["Identifier"].(string)),
				})
				require.NoError(t, err, "DescribeDBInstances should succeed after terraform apply")
				require.Len(t, out.DBInstances, 1)
				assert.Equal(t, vars["Identifier"].(string), aws.ToString(out.DBInstances[0].DBInstanceIdentifier))
				assert.Equal(t, "postgres", aws.ToString(out.DBInstances[0].Engine))
				assert.Equal(t, "available", aws.ToString(out.DBInstances[0].DBInstanceStatus))
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

// TestTerraform_Lambda provisions a Lambda function and verifies it exists.
func TestTerraform_Lambda(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "lambda/success",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				// Create a minimal zip file containing a Python handler.
				var buf bytes.Buffer
				zw := zip.NewWriter(&buf)
				f, err := zw.Create("index.py")
				require.NoError(t, err)
				_, err = f.Write([]byte("def handler(event, context):\n    return {}\n"))
				require.NoError(t, err)
				require.NoError(t, zw.Close())

				zipPath := filepath.Join(dir, "function.zip")
				require.NoError(t, os.WriteFile(zipPath, buf.Bytes(), 0o644))

				return map[string]any{
					"FuncName": "tf-lambda-" + id,
					"RoleName": "tf-lambda-role-" + id,
					"ZipPath":  zipPath,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createLambdaClient(t)
				out, err := client.GetFunction(ctx, &lambdasvc.GetFunctionInput{
					FunctionName: aws.String(vars["FuncName"].(string)),
				})
				require.NoError(t, err, "GetFunction should succeed after terraform apply")
				require.NotNil(t, out.Configuration)
				assert.Equal(t, vars["FuncName"].(string), aws.ToString(out.Configuration.FunctionName))
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

// TestTerraform_IAM provisions an IAM role, policy, and attachment and verifies the role.
func TestTerraform_IAM(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "iam/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"RoleName":   "tf-role-" + id,
					"PolicyName": "tf-policy-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createIAMClient(t)
				out, err := client.GetRole(ctx, &iamsvc.GetRoleInput{
					RoleName: aws.String(vars["RoleName"].(string)),
				})
				require.NoError(t, err, "GetRole should succeed after terraform apply")
				require.NotNil(t, out.Role)
				assert.Equal(t, vars["RoleName"].(string), aws.ToString(out.Role.RoleName))
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

// TestTerraform_SNSSQSSubscription provisions an SNS topic, SQS queue, and
// subscription, and verifies the topic exists.
func TestTerraform_SNSSQSSubscription(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "sns_sqs/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"TopicName": "tf-topic-" + id,
					"QueueName": "tf-queue-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSNSClient(t)
				out, err := client.GetTopicAttributes(ctx, &snssvc.GetTopicAttributesInput{
					TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:" + vars["TopicName"].(string)),
				})
				require.NoError(t, err, "GetTopicAttributes should succeed after terraform apply")
				assert.NotEmpty(t, out.Attributes)
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

// TestTerraform_KMS provisions a KMS key and alias and verifies the key exists.
func TestTerraform_KMS(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "kms/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"KeyDesc":   "tf-test-key-" + id,
					"AliasName": "alias/tf-test-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createKMSClient(t)
				out, err := client.DescribeKey(ctx, &kmssvc.DescribeKeyInput{
					KeyId: aws.String(vars["AliasName"].(string)),
				})
				require.NoError(t, err, "DescribeKey should succeed after terraform apply")
				require.NotNil(t, out.KeyMetadata)
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

// TestTerraform_SecretsManager provisions a secret and version and verifies the secret.
func TestTerraform_SecretsManager(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "secretsmanager/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"SecretName": "tf-secret-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSecretsManagerClient(t)
				out, err := client.DescribeSecret(ctx, &secretssvc.DescribeSecretInput{
					SecretId: aws.String(vars["SecretName"].(string)),
				})
				require.NoError(t, err, "DescribeSecret should succeed after terraform apply")
				assert.Equal(t, vars["SecretName"].(string), aws.ToString(out.Name))
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

// TestTerraform_SSMParameter provisions an SSM parameter and verifies it exists.
func TestTerraform_SSMParameter(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ssm/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"ParamName": "/tf/test/" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSSMClient(t)
				out, err := client.GetParameter(ctx, &ssmsvc.GetParameterInput{
					Name: aws.String(vars["ParamName"].(string)),
				})
				require.NoError(t, err, "GetParameter should succeed after terraform apply")
				require.NotNil(t, out.Parameter)
				assert.Equal(t, vars["ParamName"].(string), aws.ToString(out.Parameter.Name))
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

// TestTerraform_Route53 provisions a hosted zone and record and verifies the zone.
func TestTerraform_Route53(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "route53/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"ZoneName": "tf-test-" + uuid.NewString()[:8] + ".example.com"}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createRoute53Client(t)
				out, err := client.ListHostedZones(ctx, &route53svc.ListHostedZonesInput{})
				require.NoError(t, err, "ListHostedZones should succeed after terraform apply")

				zoneName := vars["ZoneName"].(string)
				found := false

				for _, zone := range out.HostedZones {
					if aws.ToString(zone.Name) == zoneName+"." || aws.ToString(zone.Name) == zoneName {
						found = true

						break
					}
				}

				assert.True(t, found, "hosted zone %q should exist after terraform apply", zoneName)
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

// TestTerraform_CloudWatchLogGroup provisions a log group and verifies it exists.
func TestTerraform_CloudWatchLogGroup(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cloudwatchlogs/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"LogGroupName": "/tf/test/" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createCloudWatchLogsClient(t)
				out, err := client.DescribeLogGroups(ctx, &cwlogssvc.DescribeLogGroupsInput{
					LogGroupNamePrefix: aws.String(vars["LogGroupName"].(string)),
				})
				require.NoError(t, err, "DescribeLogGroups should succeed after terraform apply")

				logGroupName := vars["LogGroupName"].(string)
				found := false

				for _, lg := range out.LogGroups {
					if aws.ToString(lg.LogGroupName) == logGroupName {
						found = true

						break
					}
				}

				assert.True(t, found, "log group %q should exist after terraform apply", logGroupName)
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

// TestTerraform_SESEmailIdentity provisions an SES email identity and verifies it is listed.
func TestTerraform_SESEmailIdentity(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ses/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Email": "tf-test-" + uuid.NewString()[:8] + "@example.com"}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSESClient(t)
				out, err := client.ListIdentities(ctx, &sessvc.ListIdentitiesInput{})
				require.NoError(t, err, "ListIdentities should succeed after terraform apply")

				email := vars["Email"].(string)
				assert.True(t, slices.Contains(out.Identities, email),
					"email identity %q should be listed after terraform apply", email)
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

// TestTerraform_DataSources provisions data sources and an S3 bucket and verifies the apply succeeds.
func TestTerraform_DataSources(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "datasources/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"BucketName": "tf-ds-test-" + uuid.NewString()[:8]}
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

// TestTerraform_StepFunctions provisions a Step Functions state machine and verifies it exists.
func TestTerraform_StepFunctions(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "stepfunctions/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"SMName":   "tf-sfn-" + id,
					"RoleName": "tf-sfn-role-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSFNClient(t)
				out, err := client.ListStateMachines(ctx, &sfnsvc.ListStateMachinesInput{})
				require.NoError(t, err, "ListStateMachines should succeed after terraform apply")

				smName := vars["SMName"].(string)
				found := false

				for _, sm := range out.StateMachines {
					if aws.ToString(sm.Name) == smName {
						found = true

						break
					}
				}

				assert.True(t, found, "state machine %q should exist after terraform apply", smName)
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

// TestTerraform_EventBridge provisions an EventBridge bus and rule and verifies the rule.
func TestTerraform_EventBridge(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "eventbridge/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"BusName":  "tf-bus-" + id,
					"RuleName": "tf-rule-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createEventBridgeClient(t)
				out, err := client.DescribeRule(ctx, &ebsvc.DescribeRuleInput{
					Name:         aws.String(vars["RuleName"].(string)),
					EventBusName: aws.String(vars["BusName"].(string)),
				})
				require.NoError(t, err, "DescribeRule should succeed after terraform apply")
				assert.Equal(t, vars["RuleName"].(string), aws.ToString(out.Name))
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

// TestTerraform_CloudWatchAlarm provisions a CloudWatch metric alarm and verifies it exists.
func TestTerraform_CloudWatchAlarm(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cloudwatch/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"AlarmName": "tf-alarm-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createCloudWatchClient(t)
				out, err := client.DescribeAlarms(ctx, &cwsvc.DescribeAlarmsInput{
					AlarmNames: []string{vars["AlarmName"].(string)},
				})
				require.NoError(t, err, "DescribeAlarms should succeed after terraform apply")
				require.Len(t, out.MetricAlarms, 1)
				assert.Equal(t, vars["AlarmName"].(string), aws.ToString(out.MetricAlarms[0].AlarmName))
				assert.Equal(t, cwtypes.ComparisonOperatorGreaterThanThreshold, out.MetricAlarms[0].ComparisonOperator)
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

// TestTerraform_Kinesis provisions a Kinesis stream and verifies it exists.
func TestTerraform_Kinesis(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "kinesis/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"StreamName": "tf-kinesis-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createKinesisClient(t)
				out, err := client.DescribeStreamSummary(ctx, &kinesissvc.DescribeStreamSummaryInput{
					StreamName: aws.String(vars["StreamName"].(string)),
				})
				require.NoError(t, err, "DescribeStreamSummary should succeed after terraform apply")
				require.NotNil(t, out.StreamDescriptionSummary)
				assert.Equal(t, vars["StreamName"].(string), aws.ToString(out.StreamDescriptionSummary.StreamName))
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

// TestTerraform_ACM provisions an ACM certificate and verifies it is listed.
func TestTerraform_ACM(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "acm/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Domain": "tf-acm-" + uuid.NewString()[:8] + ".example.com"}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createACMClient(t)
				out, err := client.ListCertificates(ctx, &acmsvc.ListCertificatesInput{})
				require.NoError(t, err, "ListCertificates should succeed after terraform apply")

				domain := vars["Domain"].(string)
				found := false

				for _, cert := range out.CertificateSummaryList {
					if aws.ToString(cert.DomainName) == domain {
						found = true

						break
					}
				}

				assert.True(t, found, "certificate for %q should exist after terraform apply", domain)
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

// TestTerraform_CloudFormation provisions a CloudFormation stack and verifies it exists.
func TestTerraform_CloudFormation(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cloudformation/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"StackName": "tf-cfn-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createCloudFormationClient(t)
				out, err := client.DescribeStacks(ctx, &cfnsvc.DescribeStacksInput{
					StackName: aws.String(vars["StackName"].(string)),
				})
				require.NoError(t, err, "DescribeStacks should succeed after terraform apply")
				require.Len(t, out.Stacks, 1)
				assert.Equal(t, vars["StackName"].(string), aws.ToString(out.Stacks[0].StackName))
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

// TestTerraform_ElastiCache provisions an ElastiCache cluster and verifies it exists.
func TestTerraform_ElastiCache(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "elasticache/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"ClusterID": "tf-ec-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createElastiCacheClient(t)
				out, err := client.DescribeCacheClusters(ctx, &elasticachesvc.DescribeCacheClustersInput{
					CacheClusterId: aws.String(vars["ClusterID"].(string)),
				})
				require.NoError(t, err, "DescribeCacheClusters should succeed after terraform apply")
				require.Len(t, out.CacheClusters, 1)
				assert.Equal(t, vars["ClusterID"].(string), aws.ToString(out.CacheClusters[0].CacheClusterId))
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

// TestTerraform_OpenSearch provisions an OpenSearch domain and verifies it exists.
func TestTerraform_OpenSearch(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "opensearch/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"DomainName": "tf-os-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createOpenSearchClient(t)
				out, err := client.DescribeDomain(ctx, &opensearchsvc.DescribeDomainInput{
					DomainName: aws.String(vars["DomainName"].(string)),
				})
				require.NoError(t, err, "DescribeDomain should succeed after terraform apply")
				require.NotNil(t, out.DomainStatus)
				assert.Equal(t, vars["DomainName"].(string), aws.ToString(out.DomainStatus.DomainName))
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

// TestTerraform_Redshift provisions a Redshift cluster and verifies it exists.
func TestTerraform_Redshift(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "redshift/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"ClusterID": "tf-rs-" + uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createRedshiftClient(t)
				out, err := client.DescribeClusters(ctx, &redshiftsvc.DescribeClustersInput{
					ClusterIdentifier: aws.String(vars["ClusterID"].(string)),
				})
				require.NoError(t, err, "DescribeClusters should succeed after terraform apply")
				require.Len(t, out.Clusters, 1)
				assert.Equal(t, vars["ClusterID"].(string), aws.ToString(out.Clusters[0].ClusterIdentifier))
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

// TestTerraform_Firehose provisions a Firehose delivery stream and verifies it exists.
func TestTerraform_Firehose(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "firehose/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"StreamName": "tf-fh-" + id,
					"RoleName":   "tf-fh-role-" + id,
					"BucketName": "tf-fh-bucket-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createFirehoseClient(t)
				out, err := client.DescribeDeliveryStream(ctx, &firehosesvc.DescribeDeliveryStreamInput{
					DeliveryStreamName: aws.String(vars["StreamName"].(string)),
				})
				require.NoError(t, err, "DescribeDeliveryStream should succeed after terraform apply")
				require.NotNil(t, out.DeliveryStreamDescription)
				assert.Equal(t,
					vars["StreamName"].(string),
					aws.ToString(out.DeliveryStreamDescription.DeliveryStreamName),
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

// TestTerraform_SWF provisions an SWF domain and verifies it is listed.
func TestTerraform_SWF(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "swf/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"DomainName": "tf-swf-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSWFClient(t)
				out, err := client.ListDomains(ctx, &swfsvc.ListDomainsInput{
					RegistrationStatus: swftypes.RegistrationStatusRegistered,
				})
				require.NoError(t, err, "ListDomains should succeed after terraform apply")
				found := false
				for _, d := range out.DomainInfos {
					if aws.ToString(d.Name) == vars["DomainName"].(string) {
						found = true

						break
					}
				}
				assert.True(t, found, "SWF domain %q should be listed", vars["DomainName"].(string))
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

// TestTerraform_ResourceGroups provisions a resource group and verifies it exists.
func TestTerraform_ResourceGroups(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "resourcegroups/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"GroupName": "tf-rg-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createResourceGroupsClient(t)
				out, err := client.GetGroup(ctx, &resourcegroupssvc.GetGroupInput{
					GroupName: aws.String(vars["GroupName"].(string)),
				})
				require.NoError(t, err, "GetGroup should succeed after terraform apply")
				require.NotNil(t, out.Group)
				assert.Equal(t, vars["GroupName"].(string), aws.ToString(out.Group.Name))
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

// TestTerraform_S3Control provisions an S3 account public access block and verifies it.
func TestTerraform_S3Control(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "s3control/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := createS3ControlClient(t)
				out, err := client.GetPublicAccessBlock(ctx, &s3controlsvc.GetPublicAccessBlockInput{
					AccountId: aws.String("000000000000"),
				})
				require.NoError(t, err, "GetPublicAccessBlock should succeed after terraform apply")
				require.NotNil(t, out.PublicAccessBlockConfiguration)
				assert.True(t, aws.ToBool(out.PublicAccessBlockConfiguration.BlockPublicAcls))
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

// TestTerraform_AWSConfig provisions a config recorder and delivery channel and verifies them.
func TestTerraform_AWSConfig(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "awsconfig/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"RoleName":     "tf-cfg-role-" + id,
					"BucketName":   "tf-cfg-bucket-" + id,
					"RecorderName": "tf-cfg-recorder-" + id,
					"ChannelName":  "tf-cfg-channel-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createAWSConfigClient(t)
				out, err := client.DescribeConfigurationRecorders(ctx, &configsvc.DescribeConfigurationRecordersInput{
					ConfigurationRecorderNames: []string{vars["RecorderName"].(string)},
				})
				require.NoError(t, err, "DescribeConfigurationRecorders should succeed after terraform apply")
				require.Len(t, out.ConfigurationRecorders, 1)
				assert.Equal(t, vars["RecorderName"].(string), aws.ToString(out.ConfigurationRecorders[0].Name))
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

// TestTerraform_Route53Resolver provisions a resolver rule and verifies it exists.
func TestTerraform_Route53Resolver(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "route53resolver/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"RuleName":   "tf-r53r-" + id,
					"EndpointID": id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createRoute53ResolverClient(t)
				out, err := client.ListResolverRules(ctx, &route53resolversvc.ListResolverRulesInput{})
				require.NoError(t, err, "ListResolverRules should succeed after terraform apply")
				found := false
				for _, r := range out.ResolverRules {
					if aws.ToString(r.Name) == vars["RuleName"].(string) {
						found = true

						break
					}
				}
				assert.True(t, found, "resolver rule %q should be listed", vars["RuleName"].(string))
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

// TestTerraform_EC2 provisions a VPC, subnet, security group, and instance, then verifies the instance.
func TestTerraform_EC2(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ec2/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"SGName": "tf-ec2-sg-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := createEC2Client(t)
				out, err := client.DescribeInstances(ctx, &ec2svc.DescribeInstancesInput{})
				require.NoError(t, err, "DescribeInstances should succeed after terraform apply")
				require.NotEmpty(t, out.Reservations, "at least one reservation should exist")
				require.NotEmpty(t, out.Reservations[0].Instances, "at least one instance should exist")
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

// TestTerraform_APIGateway provisions a REST API, resource, method, integration, and deployment.
func TestTerraform_APIGateway(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "apigateway/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"APIName": "tf-apigw-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createAPIGatewayClient(t)
				out, err := client.GetRestApis(ctx, &apigwsvc.GetRestApisInput{})
				require.NoError(t, err, "GetRestApis should succeed after terraform apply")
				found := false
				for _, api := range out.Items {
					if aws.ToString(api.Name) == vars["APIName"].(string) {
						found = true

						break
					}
				}
				assert.True(t, found, "REST API %q should be listed", vars["APIName"].(string))
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

// TestTerraform_Scheduler provisions an EventBridge Scheduler schedule and verifies it exists.
func TestTerraform_Scheduler(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "scheduler/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"RoleName":     "tf-sched-role-" + id,
					"ScheduleName": "tf-sched-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createSchedulerClient(t)
				out, err := client.GetSchedule(ctx, &schedulersvc.GetScheduleInput{
					Name: aws.String(vars["ScheduleName"].(string)),
				})
				require.NoError(t, err, "GetSchedule should succeed after terraform apply")
				assert.Equal(t, vars["ScheduleName"].(string), aws.ToString(out.Name))
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
