package awsconfig_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestTagResource(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.TagResource("arn:aws:config::123:rule/r1", []awsconfig.Tag{{Key: "env", Value: "prod"}})
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}

	tags := b.ListTagsForResource("arn:aws:config::123:rule/r1")
	if len(tags) != 1 || tags[0].Key != "env" || tags[0].Value != "prod" {
		t.Fatalf("ListTagsForResource: got %v, want [{env prod}]", tags)
	}
}

func TestTagResource_Update(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	arn := "arn:aws:config::123:rule/r1"

	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "prod"}})
	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "staging"}, {Key: "owner", Value: "team"}})

	tags := b.ListTagsForResource(arn)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d: %v", len(tags), tags)
	}

	for _, tag := range tags {
		if tag.Key == "env" && tag.Value != "staging" {
			t.Errorf("env tag not updated: got %q", tag.Value)
		}
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	arn := "arn:aws:config::123:rule/r1"

	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "team"}})
	_ = b.UntagResource(arn, []string{"env"})

	tags := b.ListTagsForResource(arn)
	if len(tags) != 1 || tags[0].Key != "owner" {
		t.Fatalf("UntagResource: got %v, want [{owner team}]", tags)
	}
}

func TestListTagsForResource_Empty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	tags := b.ListTagsForResource("arn:missing")
	if len(tags) != 0 {
		t.Fatalf("expected empty tags, got %v", tags)
	}
}
