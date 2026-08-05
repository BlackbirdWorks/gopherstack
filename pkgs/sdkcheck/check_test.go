package sdkcheck_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
)

// fakeClient simulates an AWS SDK v2 Client; pointer receivers match SDK convention.
type fakeClient struct{}

func (*fakeClient) GetItem()    {}
func (*fakeClient) PutItem()    {}
func (*fakeClient) DeleteItem() {}
func (*fakeClient) Options()    {} // must be excluded by BuildSDKMethodSet

// fakeClientEmpty has only Options, so its method set is empty.
type fakeClientEmpty struct{}

func (*fakeClientEmpty) Options() {}

func TestBuildSDKMethodSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input any
		want  map[string]bool
		name  string
	}{
		{
			name:  "excludes_Options_and_includes_rest",
			input: &fakeClient{},
			want:  map[string]bool{"GetItem": true, "PutItem": true, "DeleteItem": true},
		},
		{
			name:  "empty_when_only_Options",
			input: &fakeClientEmpty{},
			want:  map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sdkcheck.BuildSDKMethodSet(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []string
		wantSet  map[string]bool
		wantDups []string
	}{
		{
			name:     "no_duplicates",
			input:    []string{"Alpha", "Beta", "Gamma"},
			wantSet:  map[string]bool{"Alpha": true, "Beta": true, "Gamma": true},
			wantDups: nil,
		},
		{
			name:     "with_duplicates",
			input:    []string{"Alpha", "Beta", "Alpha"},
			wantSet:  map[string]bool{"Alpha": true, "Beta": true},
			wantDups: []string{"Alpha"},
		},
		{
			name:     "multiple_duplicates_sorted",
			input:    []string{"Zulu", "Alpha", "Zulu", "Alpha"},
			wantSet:  map[string]bool{"Alpha": true, "Zulu": true},
			wantDups: []string{"Alpha", "Zulu"},
		},
		{
			name:     "empty_input",
			input:    nil,
			wantSet:  map[string]bool{},
			wantDups: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotSet, gotDups := sdkcheck.BuildSet(tt.input)
			assert.Equal(t, tt.wantSet, gotSet)
			assert.Equal(t, tt.wantDups, gotDups)
		})
	}
}

func TestFindStale(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		notImpl   map[string]bool
		sdkMethod map[string]bool
		want      []string
	}{
		{
			name:      "no_stale_entries",
			notImpl:   map[string]bool{"GetItem": true, "PutItem": true},
			sdkMethod: map[string]bool{"GetItem": true, "PutItem": true, "DeleteItem": true},
			want:      nil,
		},
		{
			name:      "one_stale_entry",
			notImpl:   map[string]bool{"GetItem_typo": true},
			sdkMethod: map[string]bool{"GetItem": true},
			want:      []string{"GetItem_typo"},
		},
		{
			name:      "multiple_stale_entries_sorted",
			notImpl:   map[string]bool{"Zulu": true, "Alpha": true},
			sdkMethod: map[string]bool{"RealMethod": true},
			want:      []string{"Alpha", "Zulu"},
		},
		{
			name:      "empty_notImpl",
			notImpl:   map[string]bool{},
			sdkMethod: map[string]bool{"GetItem": true},
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sdkcheck.FindStale(tt.notImpl, tt.sdkMethod)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFindOverlapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    map[string]bool
		b    map[string]bool
		want []string
	}{
		{
			name: "no_overlap",
			a:    map[string]bool{"GetItem": true, "PutItem": true},
			b:    map[string]bool{"DeleteItem": true},
			want: nil,
		},
		{
			name: "one_overlap",
			a:    map[string]bool{"GetItem": true, "PutItem": true},
			b:    map[string]bool{"PutItem": true, "DeleteItem": true},
			want: []string{"PutItem"},
		},
		{
			name: "multiple_overlaps_sorted",
			a:    map[string]bool{"Zulu": true, "Alpha": true, "Beta": true},
			b:    map[string]bool{"Alpha": true, "Zulu": true},
			want: []string{"Alpha", "Zulu"},
		},
		{
			name: "empty_sets",
			a:    map[string]bool{},
			b:    map[string]bool{},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sdkcheck.FindOverlapping(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFindUnaccounted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sdkMethods   map[string]bool
		supportedSet map[string]bool
		notImplSet   map[string]bool
		want         []string
	}{
		{
			name:         "all_accounted",
			sdkMethods:   map[string]bool{"GetItem": true, "PutItem": true},
			supportedSet: map[string]bool{"GetItem": true},
			notImplSet:   map[string]bool{"PutItem": true},
			want:         nil,
		},
		{
			name:         "one_unaccounted",
			sdkMethods:   map[string]bool{"GetItem": true, "PutItem": true, "DeleteItem": true},
			supportedSet: map[string]bool{"GetItem": true},
			notImplSet:   map[string]bool{"PutItem": true},
			want:         []string{"DeleteItem"},
		},
		{
			name:         "multiple_unaccounted_sorted",
			sdkMethods:   map[string]bool{"Zulu": true, "Alpha": true, "GetItem": true},
			supportedSet: map[string]bool{"GetItem": true},
			notImplSet:   map[string]bool{},
			want:         []string{"Alpha", "Zulu"},
		},
		{
			name:         "empty_sdk",
			sdkMethods:   map[string]bool{},
			supportedSet: map[string]bool{},
			notImplSet:   map[string]bool{},
			want:         nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sdkcheck.FindUnaccounted(tt.sdkMethods, tt.supportedSet, tt.notImplSet)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCheckCompleteness_Pass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sdkClientPtr   any
		supportedOps   []string
		notImplemented []string
	}{
		{
			name:           "all_ops_in_supported",
			sdkClientPtr:   &fakeClient{},
			supportedOps:   []string{"GetItem", "PutItem", "DeleteItem"},
			notImplemented: nil,
		},
		{
			name:           "all_ops_in_notImplemented",
			sdkClientPtr:   &fakeClient{},
			supportedOps:   nil,
			notImplemented: []string{"GetItem", "PutItem", "DeleteItem"},
		},
		{
			name:           "split_between_lists",
			sdkClientPtr:   &fakeClient{},
			supportedOps:   []string{"GetItem"},
			notImplemented: []string{"PutItem", "DeleteItem"},
		},
		{
			name:           "empty_client",
			sdkClientPtr:   &fakeClientEmpty{},
			supportedOps:   nil,
			notImplemented: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sdkcheck.CheckCompleteness(t, tt.sdkClientPtr, tt.supportedOps, tt.notImplemented)
		})
	}
}

// TestCheckCompleteness_FailureOnNil verifies a nil sdkClientPtr fails cleanly.
func TestCheckCompleteness_FailureOnNil(t *testing.T) {
	t.Parallel()

	spy := newSpyT(t)
	sdkcheck.CheckCompleteness(spy, nil, nil, nil)
	require.True(t, spy.Failed(), "CheckCompleteness should report failure when sdkClientPtr is nil")
}

// TestCheckCompleteness_FailureOnNonPointer verifies a non-pointer sdkClientPtr is rejected.
func TestCheckCompleteness_FailureOnNonPointer(t *testing.T) {
	t.Parallel()

	spy := newSpyT(t)
	sdkcheck.CheckCompleteness(spy, fakeClient{}, nil, nil) // value, not pointer
	require.True(t, spy.Failed(), "CheckCompleteness should report failure when sdkClientPtr is not a pointer")
}

// TestCheckCompleteness_FailsOnUnallowlistedPhantomOp verifies a supportedOps
// entry with no matching SDK method fails unless allowlisted (mirrors the
// real-world EMR bug: GetSupportedOperations() listed a nonexistent op).
func TestCheckCompleteness_FailsOnUnallowlistedPhantomOp(t *testing.T) {
	t.Parallel()

	spy := newSpyT(t)
	sdkcheck.CheckCompleteness(
		spy, &fakeClient{},
		[]string{"GetItem", "PutItem", "DeleteItem", "ListTagsForResource"},
		nil,
	)
	require.True(t, spy.Failed(),
		"CheckCompleteness should fail the test for a phantom op that is not in phantomAllowlist")
}

// spyT wraps a [testing.TB] to intercept failure calls so we can test that
// [sdkcheck.CheckCompleteness] correctly reports errors for bad inputs, without
// propagating the failures to the parent test.
type spyT struct {
	testing.TB

	logs   []string
	failed bool
}

func newSpyT(tb testing.TB) *spyT {
	tb.Helper()

	return &spyT{TB: tb}
}

func (s *spyT) Helper()                   {}
func (s *spyT) Errorf(_ string, _ ...any) { s.failed = true }
func (s *spyT) Fatalf(_ string, _ ...any) { s.failed = true }
func (s *spyT) FailNow()                  { s.failed = true }
func (s *spyT) Fatal(_ ...any)            { s.failed = true }
func (s *spyT) Error(_ ...any)            { s.failed = true }
func (s *spyT) Fail()                     { s.failed = true }
func (s *spyT) Failed() bool              { return s.failed }
func (s *spyT) Log(args ...any)           { s.logs = append(s.logs, fmt.Sprint(args...)) }
func (s *spyT) Logf(format string, args ...any) {
	s.logs = append(s.logs, fmt.Sprintf(format, args...))
}
func (s *spyT) Name() string             { return s.TB.Name() }
func (s *spyT) Cleanup(f func())         { s.TB.Cleanup(f) }
func (s *spyT) Skip(_ ...any)            {}
func (s *spyT) Skipf(_ string, _ ...any) {}
func (s *spyT) SkipNow()                 {}
func (s *spyT) Skipped() bool            { return false }
func (s *spyT) TempDir() string          { return s.TB.TempDir() }
func (s *spyT) Setenv(_, _ string)       {}
func (s *spyT) Unsetenv(_ string)        {}
func (s *spyT) Chdir(dir string)         { s.TB.Chdir(dir) }
func (s *spyT) Context() context.Context { return context.Background() }
