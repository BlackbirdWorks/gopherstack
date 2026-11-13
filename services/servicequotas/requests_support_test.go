package servicequotas_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	sqtypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errDeskLimit = errors.New("limit")

type fakeDesk struct {
	resolved map[string]bool
	err      error
	opened   []string
}

func (d *fakeDesk) OpenCase(subject, _ string) (string, error) {
	if d.err != nil {
		return "", d.err
	}

	d.opened = append(d.opened, subject)

	return "0000000042", nil
}

func (d *fakeDesk) CaseResolved(id string) bool { return d.resolved[id] }

func TestRequests_SupportDesk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, d *fakeDesk)
		name string
	}{
		{
			name: "case_id_comes_from_desk",
			run: func(t *testing.T, f *fixture, d *fakeDesk) {
				t.Helper()

				req := requestIncrease(t, f, 20, nil)
				f.clock.advance(600 * time.Millisecond)

				got := getRequest(t, f, req.Id)
				assert.Equal(t, "0000000042", aws.ToString(got.CaseId))
				assert.Equal(t, sqtypes.RequestStatusCaseOpened, got.Status)
				assert.Len(t, d.opened, 1)
			},
		},
		{
			name: "resolved_case_closes_request",
			run: func(t *testing.T, f *fixture, d *fakeDesk) {
				t.Helper()

				req := requestIncrease(t, f, 20, nil)
				f.clock.advance(600 * time.Millisecond)
				require.Equal(t, sqtypes.RequestStatusCaseOpened, getRequest(t, f, req.Id).Status)

				d.resolved["0000000042"] = true
				f.clock.advance(time.Hour)

				assert.Equal(t, sqtypes.RequestStatusCaseClosed, getRequest(t, f, req.Id).Status)
				assert.InDelta(t, 5, appliedValue(t, f), 0)

				again := requestIncrease(t, f, 25, nil)
				assert.NotEqual(t, aws.ToString(req.Id), aws.ToString(again.Id))
			},
		},
		{
			name: "desk_failure_fails_create_support_case",
			run: func(t *testing.T, f *fixture, d *fakeDesk) {
				t.Helper()

				d.err = errDeskLimit
				req := requestIncrease(t, f, 20, aws.Bool(false))

				_, err := f.client.CreateSupportCase(t.Context(), &sqsdk.CreateSupportCaseInput{RequestId: req.Id})
				assert.Equal(t, "InvalidResourceStateException", apiCode(err))
				assert.Equal(t, sqtypes.RequestStatusPending, getRequest(t, f, req.Id).Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			d := &fakeDesk{resolved: map[string]bool{}}
			f.backend.SetSupportDesk(d)
			tt.run(t, f, d)
		})
	}
}
