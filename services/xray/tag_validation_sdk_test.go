package xray_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagResource_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{name: "valid", key: "team", value: "a"},
		{name: "empty_value", key: "team", value: ""},
		{name: "reserved_prefix", key: "aws:team", value: "a", wantErr: true},
		{name: "reserved_prefix_upper", key: "AWS:team", value: "a", wantErr: true},
		{name: "key_too_long", key: strings.Repeat("k", 129), value: "a", wantErr: true},
		{name: "key_max", key: strings.Repeat("k", 128), value: "a"},
		{name: "value_too_long", key: "team", value: strings.Repeat("v", 257), wantErr: true},
		{name: "empty_key", key: "", value: "a", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestXRayClient(t)
			g, err := c.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{GroupName: aws.String("grp")})
			require.NoError(t, err)

			tag := xraytypes.Tag{Key: aws.String(tt.key), Value: aws.String(tt.value)}

			_, err = c.TagResource(t.Context(), &xraysdk.TagResourceInput{
				ResourceARN: g.Group.GroupARN,
				Tags:        []xraytypes.Tag{tag},
			})
			_, createErr := c.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{
				GroupName: aws.String("grp2"),
				Tags:      []xraytypes.Tag{tag},
			})

			if tt.wantErr {
				var ire *xraytypes.InvalidRequestException
				require.ErrorAs(t, err, &ire)
				require.ErrorAs(t, createErr, &ire)

				return
			}

			assert.NoError(t, err)
			assert.NoError(t, createErr)
		})
	}
}
