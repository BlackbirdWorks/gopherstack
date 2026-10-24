package replicator

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEC2Endpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		base string
		reg  string
		want string
	}{
		{name: "regional", reg: "eu-west-1", want: "https://ec2.eu-west-1.amazonaws.com"},
		{name: "china", reg: "cn-north-1", want: "https://ec2.cn-north-1.amazonaws.com.cn"},
		{name: "override", reg: "us-east-1", base: "http://localhost:4566", want: "http://localhost:4566"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := aws.Config{Region: tt.reg}
			if tt.base != "" {
				cfg.BaseEndpoint = aws.String(tt.base)
			}

			assert.Equal(t, tt.want, ec2Endpoint(cfg))
		})
	}
}

func TestEC2WireIDsWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    []string
		name    string
		body    string
		wantErr string
		status  int
	}{
		{
			name: "subnets", status: 200,
			body: `<DescribeSubnetsResponse><subnetSet><item><subnetId>subnet-1</subnetId></item>` +
				`<item><subnetId>subnet-2</subnetId></item></subnetSet></DescribeSubnetsResponse>`,
			want: []string{"subnet-1", "subnet-2"},
		},
		{
			name:    "error_code",
			status:  400,
			body:    `<Response><Errors><Error><Code>AuthFailure</Code><Message>no</Message></Error></Errors></Response>`,
			wantErr: "AuthFailure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				assert.Equal(t, "DescribeSubnets", r.Form.Get("Action"))
				assert.Equal(t, "subnet-id", r.Form.Get("Filter.1.Name"))
				assert.NotEmpty(t, r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "text/xml")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(srv.Close)

			w := newEC2Wire(aws.Config{
				Region: "us-east-1", BaseEndpoint: aws.String(srv.URL),
				Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""),
			}, srv.Client())

			got, err := w.idsWhere(
				t.Context(),
				"DescribeSubnets",
				"Subnets",
				"SubnetId",
				"subnet-id",
				[]string{"subnet-1"},
			)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, errCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
