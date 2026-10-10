package workspaces_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestImportWorkspaceImage_Applications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		apps []string
		want int
	}{
		{name: "valid", apps: []string{"Microsoft_Office_2019"}, want: http.StatusOK},
		{name: "none", want: http.StatusOK},
		{name: "unknown", apps: []string{"Microsoft_Office_2007"}, want: http.StatusBadRequest},
		{name: "two", apps: []string{"Microsoft_Office_2016", "Microsoft_Office_2019"}, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Ec2ImageId":       "ami-12345678",
				"ImageName":        "imported",
				"ImageDescription": "d",
				"IngestionProcess": "BYOL_REGULAR",
			}
			if tt.apps != nil {
				body["Applications"] = tt.apps
			}

			rec := doTargetRequest(t, h, "ImportWorkspaceImage", body)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestDescribeApplications_FilterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want int
	}{
		{name: "no filters", body: map[string]any{}, want: http.StatusOK},
		{name: "valid license", body: map[string]any{"LicenseType": "LICENSED"}, want: http.StatusOK},
		{name: "bad license", body: map[string]any{"LicenseType": "FREE"}, want: http.StatusBadRequest},
		{name: "bad compute", body: map[string]any{"ComputeTypeNames": []string{"HUGE"}}, want: http.StatusBadRequest},
		{name: "bad os", body: map[string]any{"OperatingSystemNames": []string{"AMIGA"}}, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTargetRequest(t, newTestHandler(t), "DescribeApplications", tt.body)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}
