package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyPairWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupBody    string
		describeBody string
		wantContains []string
		wantMissing  []string
	}{
		{
			name: "create_time_tags_visible_on_describe",
			setupBody: "Action=CreateKeyPair&Version=2016-11-15&KeyName=tagged-key" +
				"&TagSpecification.1.ResourceType=key-pair" +
				"&TagSpecification.1.Tag.1.Key=Team&TagSpecification.1.Tag.1.Value=infra",
			describeBody: "Action=DescribeKeyPairs&Version=2016-11-15&KeyName.1=tagged-key",
			wantContains: []string{"<key>Team</key>", "<value>infra</value>"},
		},
		{
			name:         "post_create_tags_visible_on_describe",
			setupBody:    "Action=CreateKeyPair&Version=2016-11-15&KeyName=post-tagged-key",
			describeBody: "Action=DescribeKeyPairs&Version=2016-11-15&KeyName.1=post-tagged-key",
			wantContains: []string{"<keyName>post-tagged-key</keyName>"},
		},
		{
			name:         "key_pair_id_and_type_rendered",
			setupBody:    "Action=CreateKeyPair&Version=2016-11-15&KeyName=id-key",
			describeBody: "Action=DescribeKeyPairs&Version=2016-11-15&KeyName.1=id-key",
			wantContains: []string{"<keyPairId>key-", "<keyType>rsa</keyType>"},
		},
		{
			name:         "include_public_key_true_returns_key",
			setupBody:    "Action=CreateKeyPair&Version=2016-11-15&KeyName=pub-key",
			describeBody: "Action=DescribeKeyPairs&Version=2016-11-15&KeyName.1=pub-key&IncludePublicKey=true",
			wantContains: []string{"<publicKey>ssh-rsa"},
		},
		{
			name:         "include_public_key_default_omits_key",
			setupBody:    "Action=CreateKeyPair&Version=2016-11-15&KeyName=nopub-key",
			describeBody: "Action=DescribeKeyPairs&Version=2016-11-15&KeyName.1=nopub-key",
			wantMissing:  []string{"<publicKey>"},
		},
		{
			name: "tag_filter_matches_after_dual_storage_fix",
			setupBody: "Action=CreateKeyPair&Version=2016-11-15&KeyName=filter-key" +
				"&TagSpecification.1.ResourceType=key-pair" +
				"&TagSpecification.1.Tag.1.Key=Env&TagSpecification.1.Tag.1.Value=prod",
			describeBody: "Action=DescribeKeyPairs&Version=2016-11-15&Filter.1.Name=tag:Env&Filter.1.Value.1=prod",
			wantContains: []string{"<keyName>filter-key</keyName>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			setupRec := postForm(t, h, tt.setupBody)
			require.Equal(t, http.StatusOK, setupRec.Code, setupRec.Body.String())

			if tt.name == "post_create_tags_visible_on_describe" {
				tagBody := "Action=CreateTags&Version=2016-11-15&ResourceId.1=post-tagged-key" +
					"&Tag.1.Key=Owner&Tag.1.Value=team-a"
				tagRec := postForm(t, h, tagBody)
				require.Equal(t, http.StatusOK, tagRec.Code, tagRec.Body.String())
			}

			describeRec := postForm(t, h, tt.describeBody)
			require.Equal(t, http.StatusOK, describeRec.Code, describeRec.Body.String())

			body := describeRec.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}

			for _, missing := range tt.wantMissing {
				assert.NotContains(t, body, missing)
			}

			if tt.name == "post_create_tags_visible_on_describe" {
				assert.Contains(t, body, "<key>Owner</key>")
				assert.Contains(t, body, "<value>team-a</value>")
			}
		})
	}
}
