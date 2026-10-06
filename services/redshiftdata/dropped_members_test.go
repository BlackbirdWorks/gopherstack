package redshiftdata_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

func roleCtx(role, session string) context.Context {
	arn := "arn:aws:sts::123456789012:assumed-role/" + role + "/" + session

	return awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Principal: &awsmeta.Principal{
			Kind: awsmeta.PrincipalKindAssumedRole, Arn: arn, SessionName: session,
		},
	})
}

func TestListStatementsAndSessions_RoleLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		caller    context.Context
		roleLevel *bool
		name      string
		want      int
	}{
		{name: "default_same_role_all_sessions", caller: roleCtx("roleA", "s1"), want: 2},
		{name: "role_level_true", caller: roleCtx("roleA", "s1"), roleLevel: aws.Bool(true), want: 2},
		{name: "role_level_false_current_session", caller: roleCtx("roleA", "s1"), roleLevel: aws.Bool(false), want: 1},
		{name: "other_role_sees_only_own", caller: roleCtx("roleB", "s9"), want: 1},
		{name: "unauthenticated_sees_all", caller: context.Background(), want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

			for i, c := range []context.Context{
				roleCtx("roleA", "s1"), roleCtx("roleA", "s2"), roleCtx("roleB", "s9"),
			} {
				_, err := b.ExecuteStatement(c, "SELECT 1", "cluster", "", "db", "", "", "",
					false, "", nil, "session-"+string(rune('a'+i)))
				require.NoError(t, err)
			}

			stmts, _, err := b.ListStatements(tt.caller, redshiftdata.ListStatementsFilter{RoleLevel: tt.roleLevel})
			require.NoError(t, err)
			assert.Len(t, stmts, tt.want)

			sessions, _, err := b.ListSessions(tt.caller, redshiftdata.ListSessionsFilter{RoleLevel: tt.roleLevel})
			require.NoError(t, err)
			assert.Len(t, sessions, tt.want)
		})
	}
}

func TestExecuteStatement_SessionKeepAlive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sessionID *string
		keepAlive *int32
		name      string
		wantID    string
		wantErr   bool
		wantMint  bool
	}{
		{name: "mints_session", keepAlive: aws.Int32(60), wantMint: true},
		{name: "keeps_supplied_session", sessionID: aws.String("sess-1"), keepAlive: aws.Int32(60), wantID: "sess-1"},
		{name: "no_keepalive_no_session"},
		{name: "keepalive_over_24h", keepAlive: aws.Int32(86401), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := redshiftdata.NewHandler(redshiftdata.NewInMemoryBackend(testAccountID, testRegion))
			client := newTestRedshiftDataSDKClient(t, h)

			out, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
				Sql: aws.String("SELECT 1"), ClusterIdentifier: aws.String("c"), Database: aws.String("db"),
				SessionId: tt.sessionID, SessionKeepAliveSeconds: tt.keepAlive,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			switch {
			case tt.wantMint:
				require.NotEmpty(t, aws.ToString(out.SessionId))

				list, listErr := client.ListSessions(t.Context(), &redshiftdatasdk.ListSessionsInput{})
				require.NoError(t, listErr)
				require.Len(t, list.Sessions, 1)
				assert.Equal(t, aws.ToString(out.SessionId), aws.ToString(list.Sessions[0].SessionId))
			case tt.wantID != "":
				assert.Equal(t, tt.wantID, aws.ToString(out.SessionId))
			default:
				assert.Nil(t, out.SessionId)
			}
		})
	}
}
