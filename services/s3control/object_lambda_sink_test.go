package s3control_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/s3control"
)

type recordingOLSink struct {
	aps     map[string]s3.StoredObjectLambdaAccessPoint
	buckets map[string]string
	mu      sync.Mutex
}

func (r *recordingOLSink) SetObjectLambdaConfig(string, string) {}

func (r *recordingOLSink) SetObjectLambdaAccessPoint(bucket string, ap s3.StoredObjectLambdaAccessPoint) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.aps[ap.Name+":"+ap.AccountID] = ap
	r.buckets[ap.Name+":"+ap.AccountID] = bucket
}

func (r *recordingOLSink) DeleteObjectLambdaAccessPoint(name, accountID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.aps, name+":"+accountID)
	delete(r.buckets, name+":"+accountID)
}

func TestObjectLambdaAccessPointSink(t *testing.T) {
	t.Parallel()

	const (
		acct   = "123456789012"
		lambda = "arn:aws:lambda:us-east-1:123456789012:function:fn"
		config = `<SupportingAccessPoint>arn:aws:s3:us-east-1:123456789012:accesspoint/base</SupportingAccessPoint>` +
			`<TransformationConfigurations><TransformationConfiguration>` +
			`<Actions><Action>GetObject</Action><Action>HeadObject</Action></Actions><ContentTransformation><AwsLambda>` +
			`<FunctionArn>` + lambda + `</FunctionArn><FunctionPayload>{"a":1}</FunctionPayload>` +
			`</AwsLambda></ContentTransformation>` +
			`</TransformationConfiguration></TransformationConfigurations>`
	)

	tests := []struct {
		name       string
		configure  bool
		wantStored bool
		deleteAP   bool
	}{
		{name: "configured", configure: true, wantStored: true},
		{name: "unconfigured", configure: false, wantStored: false},
		{name: "deleted", configure: true, deleteAP: true, wantStored: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sink := &recordingOLSink{
				aps:     map[string]s3.StoredObjectLambdaAccessPoint{},
				buckets: map[string]string{},
			}
			b := s3control.NewInMemoryBackend()
			b.SetObjectLambdaConfigSink(sink)
			b.CreateAccessPoint(acct, "base", "my-bucket")
			ap := b.CreateAccessPointForObjectLambda(acct, "olap")

			if tt.configure {
				require.NoError(t, b.PutAccessPointConfigurationForObjectLambda(acct, "olap", config))
			}

			if tt.deleteAP {
				require.NoError(t, b.DeleteAccessPointForObjectLambda(acct, "olap"))
			}

			sink.mu.Lock()
			defer sink.mu.Unlock()

			got, ok := sink.aps["olap:"+acct]
			assert.Equal(t, tt.wantStored, ok)

			if tt.wantStored {
				assert.Equal(t, "my-bucket", sink.buckets["olap:"+acct])
				assert.Equal(t, lambda, got.LambdaARN)
				assert.Equal(t, []string{"GetObject", "HeadObject"}, got.Actions)
				assert.JSONEq(t, `{"a":1}`, got.Payload)
				assert.Equal(t, "arn:aws:s3:us-east-1:123456789012:accesspoint/base", got.SupportingAccessPointARN)
				assert.Equal(t, ap.Alias.Value, got.Alias)
			}
		})
	}
}
