package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_StatementOmitsSessionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
	}{
		{name: "print", code: "print(1)"},
		{name: "select", code: "select 1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newGlueHandler(t)
			dispatchNewOp(t, h, "CreateSession", map[string]any{
				"Id": "s", "Role": "r", "Command": map[string]any{"Name": "glueetl"},
			})
			run := dispatchNewOp(t, h, "RunStatement", map[string]any{"SessionId": "s", "Code": tc.code})
			id := run["Id"]

			got := dispatchNewOp(t, h, "GetStatement", map[string]any{"SessionId": "s", "Id": id})
			assert.NotContains(t, got["Statement"], "SessionId")

			list := dispatchNewOp(t, h, "ListStatements", map[string]any{"SessionId": "s"})
			stmts, ok := list["Statements"].([]any)
			require.True(t, ok)
			require.Len(t, stmts, 1)
			assert.NotContains(t, stmts[0], "SessionId")
		})
	}
}

func TestSDK_StatementFieldsSurvive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
	}{
		{name: "print", code: "print(1)"},
		{name: "select", code: "select 1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
				Id: aws.String("s"), Role: aws.String("r"), Command: &types.SessionCommand{Name: aws.String("glueetl")},
			})
			require.NoError(t, err)

			run, err := client.RunStatement(ctx, &gluesdk.RunStatementInput{
				SessionId: aws.String("s"), Code: aws.String(tc.code),
			})
			require.NoError(t, err)

			got, err := client.GetStatement(ctx, &gluesdk.GetStatementInput{SessionId: aws.String("s"), Id: run.Id})
			require.NoError(t, err)
			assert.Equal(t, run.Id, got.Statement.Id)
			assert.Equal(t, tc.code, aws.ToString(got.Statement.Code))
			assert.NotEmpty(t, string(got.Statement.State))

			list, err := client.ListStatements(ctx, &gluesdk.ListStatementsInput{SessionId: aws.String("s")})
			require.NoError(t, err)
			require.Len(t, list.Statements, 1)
			assert.Equal(t, tc.code, aws.ToString(list.Statements[0].Code))
		})
	}
}
