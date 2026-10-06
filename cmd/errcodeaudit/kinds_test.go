package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanFixtures_Kinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fixture   string
		code      string
		wantKind  string
		confident bool
	}{
		{"respfield", "ItemFailedCode", kindResponseField, false},
		{"respfield", "RowFailedCode", kindResponseField, false},
		{"respfield", "EnvelopeInventedCode", "", true},
		{"respfield", "PayloadErrorCode", "", true},
		{"header", "HeaderOutcomeBad", kindHeaderValue, false},
		{"header", "PlainClassifierCode", "", false},
		{"header", "ErrortypeHeaderCode", "", false},
		{"unrouted", "OrphanThingException", kindUnroutedDead, false},
		{"unrouted", "ChainThingException", kindUnroutedDead, false},
		{"unrouted", "RoutedThingException", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.fixture+"/"+tt.code, func(t *testing.T) {
			t.Parallel()

			got := scanFixture(t, tt.fixture)
			require.Contains(t, got, tt.code)
			assert.Equal(t, tt.wantKind, got[tt.code].Kind)
			assert.Equal(t, tt.confident, got[tt.code].Confident)
		})
	}
}

func TestMarkRecorded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
		want bool
	}{
		{"whole word", "RecordedCode", true},
		{"substring only", "Recorded", false},
		{"absent", "OtherCode", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := []finding{{File: "services/parity/handler.go", Code: tt.code}}
			markRecorded("testdata", fs)
			assert.Equal(t, tt.want, fs[0].Recorded)
		})
	}
}

func TestExitCodeIgnoresRecorded(t *testing.T) {
	t.Parallel()

	assert.Equal(t, exitConfidence, exitCode([]finding{{Confident: true}}))
	assert.Equal(t, exitClean, exitCode([]finding{{Confident: true, Recorded: true}}))
	assert.Equal(t, exitClean, exitCode([]finding{{Confident: false}}))
}
