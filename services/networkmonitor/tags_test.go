package networkmonitor_test

import (
	"context"
	"testing"
)

func TestTagging(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	ctx := context.Background()

	m, err := b.CreateMonitor(ctx, "tagged-mon", nil, nil, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	tags, err := b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags: %v", err)
	}

	if tags["env"] != "test" {
		t.Errorf("tag env: got %q, want test", tags["env"])
	}

	if tagErr := b.TagResource(ctx, m.MonitorArn, map[string]string{"team": "sre"}); tagErr != nil {
		t.Fatalf("tag resource: %v", tagErr)
	}

	tags, err = b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags after add: %v", err)
	}

	if tags["team"] != "sre" {
		t.Errorf("tag team: got %q, want sre", tags["team"])
	}

	if untagErr := b.UntagResource(ctx, m.MonitorArn, []string{"env"}); untagErr != nil {
		t.Fatalf("untag: %v", untagErr)
	}

	tags, err = b.ListTagsForResource(ctx, m.MonitorArn)
	if err != nil {
		t.Fatalf("list tags after remove: %v", err)
	}

	if _, ok := tags["env"]; ok {
		t.Error("expected env tag removed")
	}
}
