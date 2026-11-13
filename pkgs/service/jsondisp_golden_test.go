package service_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type goldenSendInput struct {
	QueueURL     string            `json:"QueueUrl"`
	MessageBody  string            `json:"MessageBody"`
	DelaySeconds *int32            `json:"DelaySeconds,omitempty"`
	Attributes   map[string]string `json:"MessageAttributes"`
	Ignored      string            `json:"-"`
}

func TestJSONErrorResponseGolden(t *testing.T) {
	t.Parallel()

	resp := service.JSONErrorResponse{Type: "com.amazonaws.sqs#QueueDoesNotExist", Message: "queue <x> & y"}
	b, err := resp.MarshalJSON()
	require.NoError(t, err)

	esc := strings.NewReplacer("<", "\\u003c", ">", "\\u003e", "&", "\\u0026").Replace("queue <x> & y")
	want := `{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"` + esc + `","Message":"` + esc + `"}`

	assert.Equal(t, want, string(b))
}

func TestHandleJSONGoldenDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		want    goldenSendInput
		wantErr string
	}{
		{
			name: "basic",
			body: `{"QueueUrl":"u","MessageBody":"b","DelaySeconds":5,"MessageAttributes":{"a":"b"}}`,
			want: goldenSendInput{
				QueueURL: "u", MessageBody: "b", DelaySeconds: new(int32(5)),
				Attributes: map[string]string{"a": "b"},
			},
		},
		{name: "case insensitive and unknown", body: `{"queueurl":"u","MESSAGEBODY":"b","extra":1}`,
			want: goldenSendInput{QueueURL: "u", MessageBody: "b"}},
		{name: "null members", body: `{"QueueUrl":null,"MessageAttributes":null}`, want: goldenSendInput{}},
		{name: "empty body", body: ``, want: goldenSendInput{}},
		{
			name:    "type mismatch",
			body:    `{"QueueUrl":5}`,
			wantErr: "json: cannot unmarshal number into Go struct field goldenSendInput.QueueUrl of type string",
		},
		{name: "truncated", body: `{"QueueUrl":`, wantErr: "unexpected end of JSON input"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := service.HandleJSON(context.Background(), []byte(tt.body),
				func(_ context.Context, in *goldenSendInput) (*goldenSendInput, error) { return in, nil })
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, &tt.want, got)
		})
	}
}
