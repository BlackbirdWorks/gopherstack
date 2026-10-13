package ec2_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	imdsTokenHdr = "X-Aws-Ec2-Metadata-Token"
	imdsTTLHdr   = "X-Aws-Ec2-Metadata-Token-Ttl-Seconds"
	imdsProfile  = "arn:aws:iam::000000000000:instance-profile/web"
)

var errNoSuchProfile = errors.New("no such profile")

type fakeIMDSPlatform struct {
	issued atomicCounter
	ttl    time.Duration
}

type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) inc() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++

	return c.n
}

func (c *atomicCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.n
}

func (p *fakeIMDSPlatform) InstanceProfileRole(name string) (ec2.IMDSProfile, error) {
	if name != "web" {
		return ec2.IMDSProfile{}, errNoSuchProfile
	}

	return ec2.IMDSProfile{
		ARN: imdsProfile, ID: "AIPAEXAMPLE", RoleName: "web-role", RoleARN: "arn:aws:iam::000000000000:role/web-role",
	}, nil
}

func (p *fakeIMDSPlatform) IssueInstanceCredentials(roleArn, session string) (ec2.RoleCredentials, error) {
	n := p.issued.inc()

	return ec2.RoleCredentials{
		AccessKeyID:     "ASIA" + strconv.Itoa(n),
		SecretAccessKey: "secret-" + roleArn + "-" + session,
		SessionToken:    "token-" + strconv.Itoa(n),
		Expires:         time.Now().Add(p.ttl),
	}, nil
}

type imdsFixture struct {
	h          *ec2.Handler
	b          *ec2.InMemoryBackend
	platform   *fakeIMDSPlatform
	instanceID string
}

func newIMDSFixture(t *testing.T) *imdsFixture {
	t.Helper()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID, h.Region = "000000000000", "us-east-1"
	platform := &fakeIMDSPlatform{ttl: time.Hour}
	h.SetIMDSPlatform(platform)

	insts, err := b.RunInstances("ami-0abc", "t3.micro", "", 1)
	require.NoError(t, err)

	return &imdsFixture{h: h, b: b, platform: platform, instanceID: insts[0].ID}
}

func (f *imdsFixture) do(method, path string, headers map[string]string) *httptest.ResponseRecorder {
	return f.doAs(f.instanceID, method, path, headers)
}

func (f *imdsFixture) doAs(id, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	f.h.IMDSHandler(id).ServeHTTP(rec, req)

	return rec
}

func (f *imdsFixture) token(t *testing.T, ttl string) string {
	t.Helper()

	rec := f.do(http.MethodPut, "/latest/api/token", map[string]string{imdsTTLHdr: ttl})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, ttl, rec.Header().Get(imdsTTLHdr))

	return rec.Body.String()
}

func TestIMDSTokenRequest(t *testing.T) {
	t.Parallel()

	const tokenPath = "/latest/api/token"

	ttl := func(v string) map[string]string { return map[string]string{imdsTTLHdr: v} }

	tests := []struct {
		headers map[string]string
		name    string
		method  string
		path    string
		want    int
	}{
		{name: "min_ttl", method: http.MethodPut, path: tokenPath, headers: ttl("1"), want: 200},
		{name: "max_ttl", method: http.MethodPut, path: tokenPath, headers: ttl("21600"), want: 200},
		{name: "ttl_zero", method: http.MethodPut, path: tokenPath, headers: ttl("0"), want: 400},
		{name: "ttl_too_big", method: http.MethodPut, path: tokenPath, headers: ttl("21601"), want: 400},
		{name: "ttl_garbage", method: http.MethodPut, path: tokenPath, headers: ttl("abc"), want: 400},
		{name: "ttl_missing", method: http.MethodPut, path: tokenPath, want: 400},
		{
			name: "forwarded", method: http.MethodPut, path: "/latest/api/token",
			headers: map[string]string{imdsTTLHdr: "60", "X-Forwarded-For": "1.2.3.4"}, want: 403,
		},
		{name: "put_other_path", method: http.MethodPut, path: "/latest/meta-data/instance-id", want: 405},
		{name: "post", method: http.MethodPost, path: "/latest/api/token", want: 405},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIMDSFixture(t)
			rec := f.do(tt.method, tt.path, tt.headers)
			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusOK {
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

func TestIMDSTokenEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tokens string
		token  string
		want   int
	}{
		{name: "optional_no_token", tokens: "optional", token: "none", want: 200},
		{name: "optional_valid", tokens: "optional", token: "valid", want: 200},
		{name: "optional_bogus", tokens: "optional", token: "bogus", want: 401},
		{name: "required_no_token", tokens: "required", token: "none", want: 401},
		{name: "required_valid", tokens: "required", token: "valid", want: 200},
		{name: "required_bogus", tokens: "required", token: "bogus", want: 401},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIMDSFixture(t)
			_, err := f.b.ModifyInstanceMetadataOptions(f.instanceID, tt.tokens, "", "", 0)
			require.NoError(t, err)

			headers := map[string]string{}

			switch tt.token {
			case "valid":
				headers[imdsTokenHdr] = f.token(t, "300")
			case "bogus":
				headers[imdsTokenHdr] = "not-a-token"
			}

			rec := f.do(http.MethodGet, "/latest/meta-data/instance-id", headers)
			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusOK {
				assert.Equal(t, f.instanceID, rec.Body.String())
			}
		})
	}
}

func TestIMDSTokenScopedToInstance(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	other, err := f.b.RunInstances("ami-0abc", "t3.micro", "", 1)
	require.NoError(t, err)

	token := f.token(t, "300")
	rec := f.doAs(other[0].ID, http.MethodGet, "/latest/meta-data/instance-id", map[string]string{imdsTokenHdr: token})
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestIMDSEndpointDisabled(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	_, err := f.b.ModifyInstanceMetadataOptions(f.instanceID, "", "disabled", "", 0)
	require.NoError(t, err)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/latest/meta-data/instance-id"},
		{http.MethodPut, "/latest/api/token"},
	} {
		rec := f.do(tc.method, tc.path, map[string]string{imdsTTLHdr: "60"})
		assert.Equal(t, http.StatusForbidden, rec.Code, tc.path)
	}

	_, err = f.b.ModifyInstanceMetadataOptions(f.instanceID, "", "enabled", "", 0)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, f.do(http.MethodGet, "/latest/meta-data/instance-id", nil).Code)
}

func TestIMDSUnknownInstance(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	assert.Equal(t, http.StatusNotFound, f.doAs("i-nope", http.MethodGet, "/latest/meta-data/", nil).Code)

	_, err := f.b.TerminateInstances([]string{f.instanceID})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, f.do(http.MethodGet, "/latest/meta-data/", nil).Code)
}

func TestIMDSMetadataPaths(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	insts := f.b.DescribeInstances([]string{f.instanceID}, "")
	require.Len(t, insts, 1)

	tests := []struct {
		name string
		path string
		want string
		code int
	}{
		{name: "instance_id", path: "/latest/meta-data/instance-id", want: f.instanceID, code: 200},
		{name: "ami_id", path: "/latest/meta-data/ami-id", want: "ami-0abc", code: 200},
		{name: "instance_type", path: "/latest/meta-data/instance-type", want: "t3.micro", code: 200},
		{name: "local_ipv4", path: "/latest/meta-data/local-ipv4", want: insts[0].PrivateIP, code: 200},
		{name: "region", path: "/latest/meta-data/placement/region", want: "us-east-1", code: 200},
		{name: "services_partition", path: "/latest/meta-data/services/partition", want: "aws", code: 200},
		{name: "dated_version", path: "/2021-07-15/meta-data/instance-id", want: f.instanceID, code: 200},
		{name: "bad_version", path: "/v9/meta-data/instance-id", code: 404},
		{name: "unknown_leaf", path: "/latest/meta-data/nonsense", code: 404},
		{name: "no_iam_without_profile", path: "/latest/meta-data/iam/info", code: 404},
		{name: "no_user_data", path: "/latest/user-data", code: 404},
		{name: "no_tags_by_default", path: "/latest/meta-data/tags/instance", code: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := f.do(http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.code, rec.Code)

			if tt.want != "" {
				assert.Equal(t, tt.want, rec.Body.String())
			}
		})
	}
}

func TestIMDSDirectoryListings(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	mac := f.do(http.MethodGet, "/latest/meta-data/mac", nil).Body.String()
	require.NotEmpty(t, mac)

	for path, want := range map[string][]string{
		"/latest/": {"dynamic/", "meta-data/"},
		"/latest/meta-data/": {
			"ami-id", "instance-id", "placement/", "network/", "block-device-mapping/",
		},
		"/latest/meta-data":                                {"instance-id"},
		"/latest/meta-data/placement/":                     {"availability-zone", "region"},
		"/latest/meta-data/network/interfaces/macs/":       {mac + "/"},
		"/latest/meta-data/network/interfaces/macs/" + mac: {"interface-id", "subnet-id", "vpc-id", "owner-id"},
		"/latest/dynamic/instance-identity/":               {"document"},
	} {
		rec := f.do(http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code, path)

		for _, entry := range want {
			assert.Contains(t, "\n"+rec.Body.String()+"\n", "\n"+entry+"\n", path)
		}
	}
}

func TestIMDSUserData(t *testing.T) {
	t.Parallel()

	raw := "#!/bin/sh\necho hi\x00\xff"
	f := newIMDSFixture(t)
	encoded := base64.StdEncoding.EncodeToString([]byte(raw))
	require.NoError(t, f.b.SetInstanceAttribute(f.instanceID, "userData", encoded))

	rec := f.do(http.MethodGet, "/latest/user-data", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, raw, rec.Body.String())
}

func TestIMDSInstanceTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
		code    int
	}{
		{name: "enabled", enabled: true, code: 200},
		{name: "disabled", code: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIMDSFixture(t)
			require.NoError(t, f.b.CreateTags([]string{f.instanceID}, map[string]string{"Name": "web-1", "env": "dev"}))

			if tt.enabled {
				_, err := f.b.ModifyInstanceMetadataOptions(f.instanceID, "", "", "enabled", 0)
				require.NoError(t, err)
			}

			list := f.do(http.MethodGet, "/latest/meta-data/tags/instance", nil)
			assert.Equal(t, tt.code, list.Code)

			val := f.do(http.MethodGet, "/latest/meta-data/tags/instance/Name", nil)
			assert.Equal(t, tt.code, val.Code)

			if tt.enabled {
				assert.Equal(t, "Name\nenv", list.Body.String())
				assert.Equal(t, "web-1", val.Body.String())
			}
		})
	}
}

func TestIMDSIdentityDocument(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	rec := f.do(http.MethodGet, "/latest/dynamic/instance-identity/document", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
	assert.Equal(t, f.instanceID, doc["instanceId"])
	assert.Equal(t, "us-east-1", doc["region"])
	assert.Equal(t, "000000000000", doc["accountId"])
	assert.Equal(t, "ami-0abc", doc["imageId"])
	assert.Equal(t, "t3.micro", doc["instanceType"])
	assert.Equal(t, "x86_64", doc["architecture"])
	assert.Equal(t, "2017-09-30", doc["version"])
	assert.Contains(t, doc, "billingProducts")
}

func TestIMDSIAMCredentials(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	_, err := f.b.AssociateIamInstanceProfile(f.instanceID, imdsProfile)
	require.NoError(t, err)

	info := f.do(http.MethodGet, "/latest/meta-data/iam/info", nil)
	require.Equal(t, http.StatusOK, info.Code)

	var infoDoc map[string]string
	require.NoError(t, json.Unmarshal(info.Body.Bytes(), &infoDoc))
	assert.Equal(t, "Success", infoDoc["Code"])
	assert.Equal(t, imdsProfile, infoDoc["InstanceProfileArn"])
	assert.Equal(t, "AIPAEXAMPLE", infoDoc["InstanceProfileId"])

	assert.Equal(t, "web-role", f.do(http.MethodGet, "/latest/meta-data/iam/security-credentials/", nil).Body.String())
	assert.Equal(t, 0, f.platform.issued.get(), "listing must not mint credentials")

	cred := f.do(http.MethodGet, "/latest/meta-data/iam/security-credentials/web-role", nil)
	require.Equal(t, http.StatusOK, cred.Code)

	var credDoc map[string]string
	require.NoError(t, json.Unmarshal(cred.Body.Bytes(), &credDoc))
	assert.Equal(t, "Success", credDoc["Code"])
	assert.Equal(t, "AWS-HMAC", credDoc["Type"])
	assert.Equal(t, "ASIA1", credDoc["AccessKeyId"])
	assert.Equal(t, "secret-arn:aws:iam::000000000000:role/web-role-"+f.instanceID, credDoc["SecretAccessKey"])
	assert.Equal(t, "token-1", credDoc["Token"])
	assert.NotEmpty(t, credDoc["Expiration"])

	again := f.do(http.MethodGet, "/latest/meta-data/iam/security-credentials/web-role", nil)
	assert.Equal(t, cred.Body.String(), again.Body.String(), "unexpired credentials are cached")
	other := f.do(http.MethodGet, "/latest/meta-data/iam/security-credentials/other", nil)
	assert.Equal(t, http.StatusNotFound, other.Code)
}

func TestIMDSCredentialsRefreshNearExpiry(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	_, err := f.b.AssociateIamInstanceProfile(f.instanceID, imdsProfile)
	require.NoError(t, err)

	synctest.Test(t, func(t *testing.T) {
		path := "/latest/meta-data/iam/security-credentials/web-role"
		first := f.do(http.MethodGet, path, nil).Body.String()

		time.Sleep(50 * time.Minute)
		assert.Equal(t, first, f.do(http.MethodGet, path, nil).Body.String())
		assert.Equal(t, 1, f.platform.issued.get())

		time.Sleep(6 * time.Minute)

		refreshed := f.do(http.MethodGet, path, nil).Body.String()
		assert.NotEqual(t, first, refreshed)
		assert.Equal(t, 2, f.platform.issued.get())
	})
}

func TestIMDSTokenExpiry(t *testing.T) {
	t.Parallel()

	f := newIMDSFixture(t)
	_, err := f.b.ModifyInstanceMetadataOptions(f.instanceID, "required", "", "", 0)
	require.NoError(t, err)

	synctest.Test(t, func(t *testing.T) {
		token := f.token(t, "10")
		headers := map[string]string{imdsTokenHdr: token}
		path := "/latest/meta-data/instance-id"

		time.Sleep(9 * time.Second)
		assert.Equal(t, http.StatusOK, f.do(http.MethodGet, path, headers).Code)

		time.Sleep(2 * time.Second)
		assert.Equal(t, http.StatusUnauthorized, f.do(http.MethodGet, path, headers).Code)
	})
}

type recordingCompute struct {
	reqs []ec2.LaunchRequest
	mu   sync.Mutex
}

func (c *recordingCompute) Launch(_ context.Context, req ec2.LaunchRequest) (ec2.LaunchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, req)

	return ec2.LaunchResult{ProviderID: "container-" + req.InstanceID}, nil
}

func (c *recordingCompute) Terminate(context.Context, string, string) error { return nil }
func (c *recordingCompute) Start(context.Context, string, string) error     { return nil }
func (c *recordingCompute) Stop(context.Context, string, string) error      { return nil }

type recordingListener struct {
	listened map[string]http.Handler
	closed   []string
	mu       sync.Mutex
}

func (l *recordingListener) Listen(id string, h http.Handler) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.listened[id] = h

	return "http://172.17.0.1:4242/" + id, nil
}

func (l *recordingListener) Close(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = append(l.closed, id)
}

func TestIMDSEndpointInjectedIntoLaunch(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	compute := &recordingCompute{}
	b.WithCompute(compute)

	h := ec2.NewHandler(b)
	h.AccountID, h.Region = "000000000000", "us-east-1"
	listener := &recordingListener{listened: map[string]http.Handler{}}
	h.SetIMDSListener(listener)

	client := newTestEC2Client(t, h)
	out, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-0abc"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)

	id := aws.ToString(out.Instances[0].InstanceId)

	compute.mu.Lock()
	require.Len(t, compute.reqs, 1)
	assert.Equal(t, "http://172.17.0.1:4242/"+id, compute.reqs[0].MetadataEndpoint)
	compute.mu.Unlock()

	listener.mu.Lock()
	served := listener.listened[id]
	listener.mu.Unlock()
	require.NotNil(t, served)

	rec := httptest.NewRecorder()
	served.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/latest/meta-data/instance-id", nil))
	assert.Equal(t, id, rec.Body.String())

	_, err = client.TerminateInstances(t.Context(), &ec2sdk.TerminateInstancesInput{InstanceIds: []string{id}})
	require.NoError(t, err)

	listener.mu.Lock()
	assert.Contains(t, listener.closed, id)
	listener.mu.Unlock()
}

func TestRunInstancesMetadataOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts     *types.InstanceMetadataOptionsRequest
		name     string
		wantCode string
		want     types.InstanceMetadataOptionsResponse
	}{
		{
			name: "defaults",
			want: types.InstanceMetadataOptionsResponse{
				HttpTokens: types.HttpTokensStateOptional, HttpEndpoint: types.InstanceMetadataEndpointStateEnabled,
				HttpPutResponseHopLimit: aws.Int32(1), InstanceMetadataTags: types.InstanceMetadataTagsStateDisabled,
				State: types.InstanceMetadataOptionsStateApplied,
			},
		},
		{
			name: "explicit",
			opts: &types.InstanceMetadataOptionsRequest{
				HttpTokens: types.HttpTokensStateRequired, HttpEndpoint: types.InstanceMetadataEndpointStateDisabled,
				HttpPutResponseHopLimit: aws.Int32(2), InstanceMetadataTags: types.InstanceMetadataTagsStateEnabled,
			},
			want: types.InstanceMetadataOptionsResponse{
				HttpTokens: types.HttpTokensStateRequired, HttpEndpoint: types.InstanceMetadataEndpointStateDisabled,
				HttpPutResponseHopLimit: aws.Int32(2), InstanceMetadataTags: types.InstanceMetadataTagsStateEnabled,
				State: types.InstanceMetadataOptionsStateApplied,
			},
		},
		{
			name:     "bad_tokens",
			opts:     &types.InstanceMetadataOptionsRequest{HttpTokens: "sometimes"},
			wantCode: "InvalidParameterValue",
		},
		{
			name:     "bad_hop_limit",
			opts:     &types.InstanceMetadataOptionsRequest{HttpPutResponseHopLimit: aws.Int32(65)},
			wantCode: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestEC2Client(t, h)

			out, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
				ImageId: aws.String("ami-0abc"), InstanceType: types.InstanceTypeT3Micro,
				MinCount: aws.Int32(1), MaxCount: aws.Int32(1), MetadataOptions: tt.opts,
			})

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantCode)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Instances[0].MetadataOptions)
			assert.Equal(t, tt.want, *out.Instances[0].MetadataOptions)

			desc, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
				InstanceIds: []string{aws.ToString(out.Instances[0].InstanceId)},
			})
			require.NoError(t, err)
			require.NotNil(t, desc.Reservations[0].Instances[0].MetadataOptions)
			assert.Equal(t, tt.want, *desc.Reservations[0].Instances[0].MetadataOptions)
		})
	}
}

func TestModifyInstanceMetadataOptionsKeepsUnsetFields(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	insts, err := b.RunInstances("ami-0abc", "t3.micro", "", 1)
	require.NoError(t, err)

	_, err = b.ModifyInstanceMetadataOptions(insts[0].ID, "required", "", "enabled", 3)
	require.NoError(t, err)

	got, err := b.ModifyInstanceMetadataOptions(insts[0].ID, "", "disabled", "", 0)
	require.NoError(t, err)
	assert.Equal(t, ec2.IMDSOptions{
		HTTPTokens: "required", HTTPEndpoint: "disabled", InstanceMetadataTags: "enabled",
		HTTPPutResponseHopLimit: 3, State: "applied",
	}, *got)

	for _, bad := range []struct {
		tokens, endpoint, tags string
		hop                    int
	}{{"nope", "", "", 0}, {"", "nope", "", 0}, {"", "", "nope", 0}, {"", "", "", 65}, {"", "", "", -1}} {
		_, err = b.ModifyInstanceMetadataOptions(insts[0].ID, bad.tokens, bad.endpoint, bad.tags, bad.hop)
		require.ErrorIs(t, err, ec2.ErrInvalidParameter)
	}
}

func TestRunInstancesMetadataOptionsAccountDefaults(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.ModifyInstanceMetadataDefaults("required", "", "enabled", 4))

	insts, err := b.RunInstances("ami-0abc", "t3.micro", "", 1)
	require.NoError(t, err)
	require.NoError(t, b.ApplyLaunchMetadataOptions(
		[]string{insts[0].ID}, ec2.MetadataOptionsInput{HTTPEndpoint: "disabled"},
	))

	got, err := b.ModifyInstanceMetadataOptions(insts[0].ID, "", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "required", got.HTTPTokens)
	assert.Equal(t, "disabled", got.HTTPEndpoint)
	assert.Equal(t, "enabled", got.InstanceMetadataTags)
	assert.Equal(t, 4, got.HTTPPutResponseHopLimit)
}
