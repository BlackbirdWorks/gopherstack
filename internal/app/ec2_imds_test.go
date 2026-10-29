package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

type endpointRecorder struct {
	endpoints map[string]string
	mu        sync.Mutex
}

func (r *endpointRecorder) Launch(_ context.Context, req ec2backend.LaunchRequest) (ec2backend.LaunchResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.endpoints[req.InstanceID] = req.MetadataEndpoint

	return ec2backend.LaunchResult{ProviderID: "container-" + req.InstanceID}, nil
}

func (r *endpointRecorder) Terminate(context.Context, string, string) error { return nil }
func (r *endpointRecorder) Start(context.Context, string, string) error     { return nil }
func (r *endpointRecorder) Stop(context.Context, string, string) error      { return nil }

func (r *endpointRecorder) endpoint(id string) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.endpoints[id]
}

type imdsStack struct {
	fx       *sfnFixture
	ec2      *ec2.Client
	recorder *endpointRecorder
}

func newIMDSStack(t *testing.T) *imdsStack {
	t.Helper()

	fx := newSFNFixture(t)
	byName := serviceByName(fx.services)
	h := ec2HandlerOf(byName)
	require.NotNil(t, h)

	bk, ok := h.Backend.(*ec2backend.InMemoryBackend)
	require.True(t, ok)

	rec := &endpointRecorder{endpoints: map[string]string{}}
	bk.WithCompute(rec)

	listeners := &ec2IMDSListeners{host: "127.0.0.1", servers: map[string]*http.Server{}}
	h.SetIMDSListener(listeners)
	t.Cleanup(listeners.closeAll)

	return &imdsStack{fx: fx, ec2: ec2.NewFromConfig(fx.cfg), recorder: rec}
}

func (s *imdsStack) addProfile(t *testing.T, role, principal string) {
	t.Helper()

	authzRole(t, s.fx, role, principal, "s3:*")

	iamc := iam.NewFromConfig(s.fx.cfg)
	_, err := iamc.CreateInstanceProfile(t.Context(), &iam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("web"),
	})
	require.NoError(t, err)

	_, err = iamc.AddRoleToInstanceProfile(t.Context(), &iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String("web"), RoleName: aws.String(role),
	})
	require.NoError(t, err)
}

func (s *imdsStack) run(t *testing.T, in *ec2.RunInstancesInput) string {
	t.Helper()

	in.ImageId, in.MinCount, in.MaxCount = aws.String("ami-0abc"), aws.Int32(1), aws.Int32(1)
	in.InstanceType = ec2types.InstanceTypeT3Micro

	out, err := s.ec2.RunInstances(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.Instances[0].InstanceId)
}

func TestEC2IMDSSDKClient(t *testing.T) {
	t.Parallel()

	s := newIMDSStack(t)
	s.addProfile(t, "web-role", "ec2.amazonaws.com")

	id := s.run(t, &ec2.RunInstancesInput{
		IamInstanceProfile: &ec2types.IamInstanceProfileSpecification{Name: aws.String("web")},
		UserData:           aws.String("IyEvYmluL3NoCmVjaG8gaGk="),
	})

	endpoint := s.recorder.endpoint(id)
	require.NotEmpty(t, endpoint)

	client := imds.New(imds.Options{Endpoint: endpoint})

	gotID, err := client.GetMetadata(t.Context(), &imds.GetMetadataInput{Path: "instance-id"})
	require.NoError(t, err)
	assert.Equal(t, id, readAll(t, gotID.Content))

	region, err := client.GetRegion(t.Context(), &imds.GetRegionInput{})
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", region.Region)

	doc, err := client.GetInstanceIdentityDocument(t.Context(), &imds.GetInstanceIdentityDocumentInput{})
	require.NoError(t, err)
	assert.Equal(t, id, doc.InstanceID)
	assert.Equal(t, "000000000000", doc.AccountID)
	assert.Equal(t, "t3.micro", doc.InstanceType)
	assert.Equal(t, "ami-0abc", doc.ImageID)

	ud, err := client.GetUserData(t.Context(), &imds.GetUserDataInput{})
	require.NoError(t, err)
	assert.Equal(t, "#!/bin/sh\necho hi", readAll(t, ud.Content))

	info, err := client.GetIAMInfo(t.Context(), &imds.GetIAMInfoInput{})
	require.NoError(t, err)
	assert.Equal(t, "Success", info.Code)
	assert.Contains(t, info.InstanceProfileArn, ":instance-profile/web")

	creds := ec2rolecreds.New(func(o *ec2rolecreds.Options) { o.Client = client })
	cfg := s.fx.cfg.Copy()
	cfg.Credentials = aws.NewCredentialsCache(creds)

	caller, err := sts.NewFromConfig(cfg).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sts::000000000000:assumed-role/web-role/"+id, aws.ToString(caller.Arn))
}

func TestEC2IMDSRoleTrust(t *testing.T) {
	t.Parallel()

	s := newIMDSStack(t)
	s.addProfile(t, "web-role", "lambda.amazonaws.com")

	id := s.run(t, &ec2.RunInstancesInput{
		IamInstanceProfile: &ec2types.IamInstanceProfileSpecification{Name: aws.String("web")},
	})

	client := imds.New(imds.Options{Endpoint: s.recorder.endpoint(id)})
	_, err := ec2rolecreds.New(func(o *ec2rolecreds.Options) { o.Client = client }).Retrieve(t.Context())
	require.Error(t, err)
}

func TestEC2IMDSTokensRequiredOverSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tokens     ec2types.HttpTokensState
		rawStatus  int
		sdkSuccess bool
	}{
		{
			name: "required", tokens: ec2types.HttpTokensStateRequired,
			rawStatus: http.StatusUnauthorized, sdkSuccess: true,
		},
		{name: "optional", tokens: ec2types.HttpTokensStateOptional, rawStatus: http.StatusOK, sdkSuccess: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newIMDSStack(t)
			id := s.run(t, &ec2.RunInstancesInput{
				MetadataOptions: &ec2types.InstanceMetadataOptionsRequest{HttpTokens: tt.tokens},
			})
			endpoint := s.recorder.endpoint(id)

			assert.Equal(t, tt.rawStatus, rawIMDSGet(t, endpoint+"/latest/meta-data/instance-id"))

			client := imds.New(imds.Options{Endpoint: endpoint, EnableFallback: aws.FalseTernary})
			_, err := client.GetMetadata(t.Context(), &imds.GetMetadataInput{Path: "instance-id"})
			assert.Equal(t, tt.sdkSuccess, err == nil)
		})
	}
}

func rawIMDSGet(t *testing.T, url string) int {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()

	b, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(b)
}

func TestEC2IMDSEndpointDisabledOverSDK(t *testing.T) {
	t.Parallel()

	s := newIMDSStack(t)
	id := s.run(t, &ec2.RunInstancesInput{
		MetadataOptions: &ec2types.InstanceMetadataOptionsRequest{
			HttpEndpoint: ec2types.InstanceMetadataEndpointStateDisabled,
		},
	})

	client := imds.New(imds.Options{Endpoint: s.recorder.endpoint(id)})
	_, err := client.GetMetadata(t.Context(), &imds.GetMetadataInput{Path: "instance-id"})
	require.Error(t, err)

	_, err = s.ec2.ModifyInstanceMetadataOptions(t.Context(), &ec2.ModifyInstanceMetadataOptionsInput{
		InstanceId: aws.String(id), HttpEndpoint: ec2types.InstanceMetadataEndpointStateEnabled,
	})
	require.NoError(t, err)

	_, err = client.GetMetadata(t.Context(), &imds.GetMetadataInput{Path: "instance-id"})
	require.NoError(t, err)
}

func TestEC2IMDSTerminateClosesListener(t *testing.T) {
	t.Parallel()

	s := newIMDSStack(t)
	id := s.run(t, &ec2.RunInstancesInput{})
	endpoint := s.recorder.endpoint(id)

	_, err := s.ec2.TerminateInstances(t.Context(), &ec2.TerminateInstancesInput{InstanceIds: []string{id}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		url := endpoint + "/latest/meta-data/instance-id"
		req, rErr := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		require.NoError(t, rErr)

		resp, dErr := testclient.New(t).Do(req)
		if dErr != nil {
			return true
		}

		_ = resp.Body.Close()

		return false
	}, authzDeadline, authzTick)
}

func TestEC2IMDSEmulatorPath(t *testing.T) {
	t.Parallel()

	s := newIMDSStack(t)
	id := s.run(t, &ec2.RunInstancesInput{})

	do := func(method, path string, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/_gopherstack/ec2/imds/"+id+path, nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		rec := httptest.NewRecorder()
		s.fx.handler.ServeHTTP(rec, req)

		return rec
	}

	get := do(http.MethodGet, "/latest/meta-data/instance-id", nil)
	assert.Equal(t, http.StatusOK, get.Code)
	assert.Equal(t, id, get.Body.String())

	token := do(http.MethodPut, "/latest/api/token", map[string]string{"X-Aws-Ec2-Metadata-Token-Ttl-Seconds": "60"})
	assert.Equal(t, http.StatusOK, token.Code)
	assert.NotEmpty(t, token.Body.String())
}
