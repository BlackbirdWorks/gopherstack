package comprehend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdvanceTrainingResourceUsesModelStatusVocabulary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, resName, from, want string
	}{
		{name: "submitted_to_training", resName: "m", from: "SUBMITTED", want: "TRAINING"},
		{name: "training_to_trained", resName: "m", from: "TRAINING", want: "TRAINED"},
		{name: "training_to_in_error", resName: "m-[fail]", from: "TRAINING", want: "IN_ERROR"},
		{name: "trained_is_terminal", resName: "m", from: "TRAINED", want: "TRAINED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Resource{Type: resourceTypeDocClassifier, Name: tt.resName, Status: tt.from}
			advanceTrainingResource(r)
			assert.Equal(t, tt.want, r.Status)
		})
	}
}
