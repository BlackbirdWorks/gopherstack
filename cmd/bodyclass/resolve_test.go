package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModuleVersion(t *testing.T) {
	t.Parallel()

	const goMod = `module github.com/example/x

go 1.26.6

require (
	github.com/aws/aws-sdk-go-v2/service/appmesh v1.38.4
	github.com/aws/aws-sdk-go-v2/service/glacier v1.35.4
)

require github.com/aws/aws-sdk-go-v2/service/dax v1.19.0
`

	tests := []struct {
		name string
		mod  string
		want string
	}{
		{name: "inside require block", mod: "appmesh", want: "v1.38.4"},
		{name: "second entry inside block", mod: "glacier", want: "v1.35.4"},
		{name: "standalone require line", mod: "dax", want: "v1.19.0"},
		{name: "unpinned module", mod: "nope", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, moduleVersion(goMod, tt.mod))
		})
	}
}

func TestSdkModsFor(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := `package appmesh

import "github.com/aws/aws-sdk-go-v2/service/appmesh"

var _ = appmesh.Client{}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

	mods, err := sdkModsFor(dir)

	require.NoError(t, err)
	assert.Equal(t, []string{"appmesh"}, mods)
}

func TestSdkModsForNoImports(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte("package empty\n"), 0o600))

	mods, err := sdkModsFor(dir)

	require.NoError(t, err)
	assert.Empty(t, mods)
}

func TestResolveModuleName(t *testing.T) {
	t.Parallel()

	repoRoot := t.TempDir()

	writeService(t, repoRoot, "appmesh", `package appmesh

import "github.com/aws/aws-sdk-go-v2/service/appmesh"

var _ = appmesh.Client{}
`)
	writeService(t, repoRoot, "dms", `package dms

import "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"

var _ = databasemigrationservice.Client{}
`)
	writeService(t, repoRoot, "opsworks", `package opsworks

func doNothing() {}
`)

	tests := []struct {
		name    string
		service string
		want    string
		wantErr bool
	}{
		{name: "directory equals module", service: "appmesh", want: "appmesh"},
		{name: "override table used when directory diverges", service: "dms", want: "databasemigrationservice"},
		{name: "no pinned sdk import", service: "opsworks", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveModuleName(repoRoot, tt.service)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func writeService(t *testing.T, repoRoot, name, src string) {
	t.Helper()

	dir := filepath.Join(repoRoot, "services", name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))
}
