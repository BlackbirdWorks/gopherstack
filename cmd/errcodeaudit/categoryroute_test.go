package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const awserrImportPrefix = `package svc

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var ErrNotFound = awserr.New("ClientException", awserr.ErrNotFound)

func errorResponse(code, msg string) map[string]string { return nil }
`

func TestApplyCategoryRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantOutput  string
		wantDemoted bool
	}{
		{
			name: "switch case emitting its own literal demotes the sentinel and keeps the output",
			body: `func handle(err error) map[string]string {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return errorResponse("InvalidRequestException", err.Error())
	}
	return nil
}`,
			wantDemoted: true,
			wantOutput:  "InvalidRequestException",
		},
		{
			name: "if guard emitting its own literal demotes",
			body: `func handle(err error) map[string]string {
	if errors.Is(err, awserr.ErrNotFound) {
		return errorResponse("InvalidRequestException", err.Error())
	}
	return nil
}`,
			wantDemoted: true,
			wantOutput:  "InvalidRequestException",
		},
		{
			name: "case reading the code from the error chain never demotes",
			body: `func handle(err error) map[string]string {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		code := errorCode(err)
		return errorResponse(code, err.Error())
	}
	return nil
}
func errorCode(err error) string { return err.Error() }`,
		},
		{
			name: "one literal case and one dynamic case on the same category never demotes",
			body: `func handle(err error, dyn bool) map[string]string {
	switch {
	case errors.Is(err, awserr.ErrNotFound) && dyn:
		return errorResponse(errorCode(err), "")
	case errors.Is(err, awserr.ErrNotFound):
		return errorResponse("InvalidRequestException", "")
	}
	return nil
}
func errorCode(err error) string { return err.Error() }`,
		},
		{
			name: "no guard on the category at all never demotes",
			body: `func handle(err error) error { return err }`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cands := extractFixture(t, awserrImportPrefix+tc.body)

			reason := mapperReasonFor(t, cands, "ClientException")
			assert.Equal(t, tc.wantDemoted, reason != "")

			if tc.wantOutput != "" {
				assert.Empty(t, mapperReasonFor(t, cands, tc.wantOutput))
			}
		})
	}
}

func TestSetupOnlySentinelDemoted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "raised only from Init is demoted",
			body: `func (s *Svc) Init(x any) error {
	if x == nil {
		return ErrNilCtx
	}
	return nil
}`,
			want: true,
		},
		{
			name: "also raised from a handler path is not demoted",
			body: `func (s *Svc) Init(x any) error { return ErrNilCtx }
func (s *Svc) handle() error { return ErrNilCtx }
var _ = (*Svc).handle`,
		},
		{
			name: "referenced from a package-level table is not demoted",
			body: `func (s *Svc) Init(x any) error { return ErrNilCtx }
var table = map[error]string{ErrNilCtx: "x"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

type Svc struct{}

var ErrNilCtx = awserr.New("InvalidParameter", awserr.ErrInvalidParameter)

` + tc.body
			cands := extractFixture(t, src)

			for _, c := range cands {
				if c.Code == "InvalidParameter" {
					assert.Equal(t, tc.want, c.DemoteReason != "")
				}
			}
		})
	}
}
