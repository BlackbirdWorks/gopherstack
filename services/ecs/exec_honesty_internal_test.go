package ecs

import (
	"strings"
	"testing"
)

// TestExecuteCommand_StreamURLIsHonest asserts the ExecuteCommand stream URL does
// not masquerade as a real, connectable AWS SSM endpoint. gopherstack has no SSM
// data channel, so the URL must use the reserved non-resolvable ".invalid" TLD
// rather than ssmmessages.<region>.amazonaws.com.
func TestExecuteCommand_StreamURLIsHonest(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tdArn := registerSimpleTaskDef(t, b, "exec-app", "nginx")
	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "ec"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	tasks, err := b.RunTask(RunTaskInput{
		Cluster:              "ec",
		TaskDefinition:       tdArn,
		Count:                1,
		EnableExecuteCommand: true,
	})
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}

	out, err := b.ExecuteCommand("ec", tasks[0].TaskArn, "app", "/bin/sh", true)
	if err != nil {
		t.Fatalf("ExecuteCommand: %v", err)
	}

	tests := []struct {
		name      string
		substr    string
		wantFound bool
	}{
		{name: "not a real ssmmessages host", substr: "amazonaws.com", wantFound: false},
		{name: "uses non-resolvable invalid TLD", substr: ".invalid", wantFound: true},
		{name: "carries the session id", substr: out.Session.SessionID, wantFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := strings.Contains(out.Session.StreamURL, tt.substr)
			if got != tt.wantFound {
				t.Errorf("StreamURL %q contains %q = %v, want %v",
					out.Session.StreamURL, tt.substr, got, tt.wantFound)
			}
		})
	}

	if out.Session.TokenValue == "" {
		t.Error("TokenValue must not be empty")
	}
}
