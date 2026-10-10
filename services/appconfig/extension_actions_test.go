package appconfig_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

var (
	errUnexpectedFunction = errors.New("unexpected function")
	errBoom               = errors.New("boom")
)

const testLambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:ext"

type recordingInvoker struct {
	err      error
	resp     string
	payloads [][]byte
	mu       sync.Mutex
}

func (r *recordingInvoker) Invoke(_ context.Context, arn string, payload []byte) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if arn != testLambdaARN {
		return nil, errUnexpectedFunction
	}

	r.payloads = append(r.payloads, payload)

	return []byte(r.resp), r.err
}

func (r *recordingInvoker) lastEvent(t *testing.T) map[string]any {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	require.NotEmpty(t, r.payloads)

	var ev map[string]any
	require.NoError(t, json.Unmarshal(r.payloads[len(r.payloads)-1], &ev))

	return ev
}

type extensionFixture struct {
	b          *appconfig.InMemoryBackend
	invoker    *recordingInvoker
	appID      string
	envID      string
	profileID  string
	strategyID string
}

func newExtensionFixture(t *testing.T, actionPoint, response string, invokeErr error) *extensionFixture {
	t.Helper()

	b := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	inv := &recordingInvoker{resp: response, err: invokeErr}
	b.SetExtensionLambdaInvoker(inv)

	app, err := b.CreateApplication("app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(app.ID, "profile", "", "hosted", "", "", "", nil, nil)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("instant", "", 0, 0, 100, "LINEAR", "NONE", nil)
	require.NoError(t, err)

	ext, err := b.CreateExtension("ext", "", map[string][]appconfig.ExtensionAction{
		actionPoint: {{Name: "act", URI: testLambdaARN}},
	}, map[string]appconfig.ExtensionParameter{"P": {Required: true}}, nil)
	require.NoError(t, err)

	_, err = b.CreateExtensionAssociation(ext.ID, "arn:aws:appconfig:us-east-1:000000000000:application/"+app.ID,
		map[string]string{"P": "assoc"}, nil, nil)
	require.NoError(t, err)

	return &extensionFixture{
		b: b, invoker: inv, appID: app.ID, envID: env.ID, profileID: profile.ID, strategyID: strategy.ID,
	}
}

func TestExtensionActions_PreCreateHostedConfigurationVersion(t *testing.T) {
	t.Parallel()

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	tests := []struct {
		invokeErr   error
		name        string
		response    string
		wantContent string
		wantErr     bool
	}{
		{name: "content_replaced", response: `{"Content":"` + b64("scrubbed") + `"}`, wantContent: "scrubbed"},
		{name: "empty_response_keeps_content", response: ``, wantContent: "original"},
		{
			name:     "error_response_rejects",
			response: `{"Error":"BadRequestError","Message":"secret found"}`,
			wantErr:  true,
		},
		{name: "function_error_rejects", invokeErr: errBoom, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newExtensionFixture(t, "PRE_CREATE_HOSTED_CONFIGURATION_VERSION", tt.response, tt.invokeErr)

			v, err := f.b.CreateHostedConfigurationVersion(
				f.appID, f.profileID, "text/plain", "d", "", []byte("original"), nil,
			)
			if tt.wantErr {
				require.ErrorIs(t, err, appconfig.ErrBadRequest)

				_, getErr := f.b.GetHostedConfigurationVersion(f.appID, f.profileID, 1)
				require.Error(t, getErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantContent, string(v.Content))

			ev := f.invoker.lastEvent(t)
			assert.Equal(t, "PreCreateHostedConfigurationVersion", ev["Type"])
			assert.Equal(t, b64("original"), ev["Content"])
			assert.Equal(t, "1", ev["ContentVersion"])
			assert.Equal(t, map[string]any{"P": "assoc"}, ev["Parameters"])
			assert.NotEmpty(t, ev["InvocationId"])
		})
	}
}

func TestExtensionActions_PreStartDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		invokeErr error
		name      string
		response  string
		wantErr   bool
	}{
		{name: "allowed", response: `{}`},
		{name: "rejected", response: `{"Error":"Blocked","Message":"change freeze"}`, wantErr: true},
		{name: "function_error", invokeErr: errBoom, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newExtensionFixture(t, "PRE_START_DEPLOYMENT", tt.response, tt.invokeErr)

			_, err := f.b.CreateHostedConfigurationVersion(
				f.appID, f.profileID, "text/plain", "", "", []byte("cfg"), nil,
			)
			require.NoError(t, err)

			d, err := f.b.StartDeploymentWithParameters(
				f.appID, f.envID, f.profileID, f.strategyID, "1", "release", nil, nil, nil,
				map[string]string{"Dyn": "x"},
			)
			if tt.wantErr {
				require.ErrorIs(t, err, appconfig.ErrBadRequest)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, int32(1), d.DeploymentNumber)

			ev := f.invoker.lastEvent(t)
			assert.Equal(t, "PreStartDeployment", ev["Type"])
			assert.Equal(t, map[string]any{"P": "assoc", "Dyn": "x"}, ev["Parameters"])
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("cfg")), ev["Content"])
			assert.EqualValues(t, 1, ev["DeploymentNumber"])
		})
	}
}

type capturePublisher struct {
	content string
}

func (c *capturePublisher) PublishConfiguration(_, _, _, content, _, _ string) error {
	c.content = content

	return nil
}

func TestExtensionActions_PreStartDeploymentTransformsDeployedContent(t *testing.T) {
	t.Parallel()

	resp := `{"Content":"` + base64.StdEncoding.EncodeToString([]byte("redacted")) + `"}`
	f := newExtensionFixture(t, "PRE_START_DEPLOYMENT", resp, nil)

	pub := &capturePublisher{}
	f.b.SetDeployedConfigurationPublisher(pub)

	_, err := f.b.CreateHostedConfigurationVersion(f.appID, f.profileID, "text/plain", "", "", []byte("secret"), nil)
	require.NoError(t, err)

	_, err = f.b.StartDeployment(f.appID, f.envID, f.profileID, f.strategyID, "1", "", nil, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "redacted", pub.content)

	stored, err := f.b.GetHostedConfigurationVersion(f.appID, f.profileID, 1)
	require.NoError(t, err)
	assert.Equal(t, "secret", string(stored.Content))
}
