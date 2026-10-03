package stepfunctions_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func BenchmarkStartSyncExecution(b *testing.B) {
	bk := stepfunctions.NewInMemoryBackend()
	sm, err := bk.CreateStateMachine(
		context.Background(), "bench-sync",
		`{"StartAt":"A","States":{
"A":{"Type":"Pass","Parameters":{"id.$":"$.id","n":"x"},"ResultPath":"$.a","Next":"B"},
"B":{"Type":"Choice","Choices":[{"Variable":"$.id","NumericGreaterThan":0,"Next":"C"}],"Default":"D"},
"C":{"Type":"Pass","End":true},"D":{"Type":"Succeed"}}}`,
		"arn:role", "EXPRESS",
	)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, execErr := bk.StartSyncExecution(sm.StateMachineArn, "", `{"id":3}`); execErr != nil {
			b.Fatal(execErr)
		}
	}
}
