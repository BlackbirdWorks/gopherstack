package gcpresourcemanager_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProjectInOrganization(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.grpc

	parent := orgName
	for _, n := range []string{"Alpha", "Beta", "Gamma", "Delta"} {
		parent = c.createFolder(t, parent, n).GetName()
	}

	c.createProject(t, "deep-proj", parent)
	c.createProject(t, "top-proj", orgName)

	tests := []struct {
		name         string
		project      string
		wantIn, want bool
	}{
		{name: "deep in folders", project: "deep-proj", wantIn: true, want: true},
		{name: "directly under org", project: "top-proj", wantIn: true, want: true},
		{name: "unknown project", project: "no-such-proj", wantIn: false, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in, known := e.backend.ProjectInOrganization(tt.project)
			assert.Equal(t, tt.wantIn, in)
			assert.Equal(t, tt.want, known)
		})
	}
}

func TestProjectInOrganizationConcurrent(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.grpc
	f := c.createFolder(t, orgName, "Race fold")
	c.createProject(t, "race-proj", f.GetName())

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			for range 50 {
				in, known := e.backend.ProjectInOrganization("race-proj")
				assert.True(t, in && known)
			}
		})
	}

	for i := range 3 {
		c.createFolder(t, f.GetName(), "Child"+string(rune(0x41+i))+"x")
	}

	wg.Wait()
}
