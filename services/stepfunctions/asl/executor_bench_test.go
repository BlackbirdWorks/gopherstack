package asl_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

const benchPipelineDef = `{"StartAt":"A","States":{
"A":{"Type":"Pass","Parameters":{"id.$":"$.id","name.$":"$.user.name","exec.$":"$$.Execution.Name"},
"ResultPath":"$.a","Next":"B"},
"B":{"Type":"Pass","Result":{"x":1,"y":[1,2,3]},"ResultSelector":{"x.$":"$.x"},"ResultPath":"$.b","Next":"C"},
"C":{"Type":"Choice","Choices":[{"Variable":"$.id","NumericGreaterThan":0,"Next":"D"}],"Default":"E"},
"D":{"Type":"Pass","InputPath":"$.user","Parameters":{"n.$":"$.name"},"OutputPath":"$.n","End":true},
"E":{"Type":"Succeed"}}}`

func BenchmarkExecutePipeline(b *testing.B) {
	sm, err := asl.Parse(benchPipelineDef)
	require.NoError(b, err)

	in := `{"id":5,"user":{"name":"bob","age":3},"items":[1,2,3,4,5]}`
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		ex := asl.NewExecutor(sm, nil, nil)
		ex.SetExecutionContext("arn:e", "e", "role", "2020-01-01T00:00:00Z", "arn:sm", "sm")

		if _, execErr := ex.Execute(ctx, "arn:e", in); execErr != nil {
			b.Fatal(execErr)
		}
	}
}

const benchMapDef = `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items",
"ItemSelector":{"v.$":"$$.Map.Item.Value","i.$":"$$.Map.Item.Index","c":"k"},
"ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"w.$":"$.v"},"End":true}}},"End":true}}}`

func BenchmarkExecuteMap(b *testing.B) {
	sm, err := asl.Parse(benchMapDef)
	require.NoError(b, err)

	in := `{"items":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]}`
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		ex := asl.NewExecutor(sm, nil, nil)
		if _, execErr := ex.Execute(ctx, "arn:e", in); execErr != nil {
			b.Fatal(execErr)
		}
	}
}
