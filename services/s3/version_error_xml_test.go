package s3_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionIDErrors_XMLElements(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "vx-bucket")

	_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket:                  aws.String("vx-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)
	mustPutObject(t, backend, "vx-bucket", "k&1", []byte("v"))

	const (
		malformed = "bad<id>&x"
		absent    = "0123456789abcdef0123456789abcdef"
	)

	malformedElems := []string{
		"<Code>InvalidArgument</Code>",
		"<Message>Invalid version id specified</Message>",
		"<ArgumentName>versionId</ArgumentName>",
		"<ArgumentValue>bad&lt;id&gt;&amp;x</ArgumentValue>",
	}
	absentElems := []string{
		"<Code>NoSuchVersion</Code>",
		"<Message>The specified version does not exist.</Message>",
		"<Key>k&amp;1</Key>",
		"<VersionId>" + absent + "</VersionId>",
	}

	tests := []struct {
		name      string
		method    string
		query     string
		versionID string
		want      []string
		wantCode  int
	}{
		{"get malformed", http.MethodGet, "", malformed, malformedElems, 400},
		{"get absent", http.MethodGet, "", absent, absentElems, 404},
		{"tagging malformed", http.MethodGet, "tagging", malformed, malformedElems, 400},
		{"tagging absent", http.MethodGet, "tagging", absent, absentElems, 404},
		{"acl malformed", http.MethodGet, "acl", malformed, malformedElems, 400},
		{"acl absent", http.MethodGet, "acl", absent, absentElems, 404},
		{"attributes malformed", http.MethodGet, "attributes", malformed, malformedElems, 400},
		{"attributes absent", http.MethodGet, "attributes", absent, absentElems, 404},
		{"legal hold malformed", http.MethodGet, "legal-hold", malformed, malformedElems, 400},
		{"retention absent", http.MethodGet, "retention", absent, absentElems, 404},
		{"delete malformed", http.MethodDelete, "", malformed, malformedElems, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target := "/vx-bucket/" + url.PathEscape("k&1") + "?"
			if tt.query != "" {
				target += tt.query + "&"
			}

			target += "versionId=" + url.QueryEscape(tt.versionID)

			req := httptest.NewRequest(tt.method, target, nil)
			req.Header.Set("X-Amz-Object-Attributes", "ETag")

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			for _, el := range tt.want {
				assert.Contains(t, rec.Body.String(), el)
			}
		})
	}
}

func TestDeleteObjects_MalformedVersionIDItem(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "dx-bucket")
	mustPutObject(t, backend, "dx-bucket", "k", []byte("v"))

	out, err := backend.DeleteObjects(t.Context(), &sdk_s3.DeleteObjectsInput{
		Bucket: aws.String("dx-bucket"),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("k"), VersionId: aws.String("garbage!")},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.Errors, 1)
	assert.Equal(t, "InvalidArgument", aws.ToString(out.Errors[0].Code))
	assert.Equal(t, "k", aws.ToString(out.Errors[0].Key))
	assert.Equal(t, "garbage!", aws.ToString(out.Errors[0].VersionId))
}
