package rds_test

import (
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

const (
	shareOwner = "111111111111"
	shareGuest = "222222222222"
	shareOther = "333333333333"
)

type shareStack struct {
	clients map[string]*rdssdk.Client
}

func newShareStack(t *testing.T) *shareStack {
	t.Helper()

	b := rds.NewInMemoryBackend("000000000000", config.DefaultRegion)
	t.Cleanup(b.Close)

	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			c.SetRequest(req.WithContext(awsmeta.Set(req.Context(), awsmeta.FromRequest(req, config.DefaultRegion))))

			return next(c)
		}
	})
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(rds.NewHandler(b)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	s := &shareStack{clients: map[string]*rdssdk.Client{}}

	for _, acct := range []string{shareOwner, shareGuest, shareOther} {
		cfg, err := awscfg.LoadDefaultConfig(t.Context(),
			awscfg.WithRegion(config.DefaultRegion),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acct, "x", "")),
		)
		require.NoError(t, err)

		s.clients[acct] = rdssdk.NewFromConfig(cfg, func(o *rdssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	}

	return s
}

func snapshotARN(acct, id string) string {
	return "arn:aws:rds:" + config.DefaultRegion + ":" + acct + ":snapshot:" + id
}

func clusterSnapshotARN(acct, id string) string {
	return "arn:aws:rds:" + config.DefaultRegion + ":" + acct + ":cluster-snapshot:" + id
}

// seedSnapshots has the owner create shared-with-guest, public and private snapshots.
func (s *shareStack) seedSnapshots(t *testing.T) {
	t.Helper()

	owner := s.clients[shareOwner]
	_, err := owner.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("src"), Engine: aws.String("postgres"),
		DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
		MasterUserPassword: aws.String("Passw0rd!x"), AllocatedStorage: aws.Int32(20),
	})
	require.NoError(t, err)

	for _, id := range []string{"snap-shared", "snap-public", "snap-private"} {
		out, cerr := owner.CreateDBSnapshot(t.Context(), &rdssdk.CreateDBSnapshotInput{
			DBInstanceIdentifier: aws.String("src"), DBSnapshotIdentifier: aws.String(id),
		})
		require.NoError(t, cerr)
		assert.Equal(t, snapshotARN(shareOwner, id), aws.ToString(out.DBSnapshot.DBSnapshotArn))
	}

	for id, value := range map[string]string{"snap-shared": shareGuest, "snap-public": "all"} {
		_, merr := owner.ModifyDBSnapshotAttribute(t.Context(), &rdssdk.ModifyDBSnapshotAttributeInput{
			DBSnapshotIdentifier: aws.String(id), AttributeName: aws.String("restore"),
			ValuesToAdd: []string{value},
		})
		require.NoError(t, merr)
	}
}

func snapshotIDs(out *rdssdk.DescribeDBSnapshotsOutput) []string {
	ids := make([]string, 0, len(out.DBSnapshots))
	for _, s := range out.DBSnapshots {
		ids = append(ids, aws.ToString(s.DBSnapshotIdentifier))
	}

	slices.Sort(ids)

	return ids
}

func TestDescribeDBSnapshotsSharing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		caller        string
		snapshotType  string
		want          []string
		includeShared bool
		includePublic bool
	}{
		{name: "owner default", caller: shareOwner, want: []string{"snap-private", "snap-public", "snap-shared"}},
		{
			name: "owner manual", caller: shareOwner, snapshotType: "manual",
			want: []string{"snap-private", "snap-public", "snap-shared"},
		},
		{name: "guest default sees nothing", caller: shareGuest, want: []string{}},
		{name: "guest include shared", caller: shareGuest, includeShared: true, want: []string{"snap-shared"}},
		{name: "guest include public", caller: shareGuest, includePublic: true, want: []string{"snap-public"}},
		{
			name: "guest include both", caller: shareGuest, includeShared: true, includePublic: true,
			want: []string{"snap-public", "snap-shared"},
		},
		{name: "guest shared type", caller: shareGuest, snapshotType: "shared", want: []string{"snap-shared"}},
		{name: "guest public type", caller: shareGuest, snapshotType: "public", want: []string{"snap-public"}},
		{name: "guest manual type", caller: shareGuest, snapshotType: "manual", want: []string{}},
		{name: "other include shared", caller: shareOther, includeShared: true, want: []string{}},
		{name: "other shared type", caller: shareOther, snapshotType: "shared", want: []string{}},
		{name: "other public type", caller: shareOther, snapshotType: "public", want: []string{"snap-public"}},
		{name: "owner shared type", caller: shareOwner, snapshotType: "shared", want: []string{}},
		{name: "owner public type", caller: shareOwner, snapshotType: "public", want: []string{"snap-public"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newShareStack(t)
			s.seedSnapshots(t)

			in := &rdssdk.DescribeDBSnapshotsInput{
				IncludeShared: aws.Bool(tt.includeShared), IncludePublic: aws.Bool(tt.includePublic),
			}
			if tt.snapshotType != "" {
				in.SnapshotType = aws.String(tt.snapshotType)
			}

			out, err := s.clients[tt.caller].DescribeDBSnapshots(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, snapshotIDs(out))
		})
	}
}

func TestSharedDBSnapshotAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		caller   string
		snapshot string
		wantCode string
	}{
		{
			name:     "describe shared by arn",
			op:       "describe",
			caller:   shareGuest,
			snapshot: snapshotARN(shareOwner, "snap-shared"),
		},
		{
			name: "describe shared by name", op: "describe", caller: shareGuest, snapshot: "snap-shared",
			wantCode: "DBSnapshotNotFound",
		},
		{
			name: "describe private by arn", op: "describe", caller: shareGuest,
			snapshot: snapshotARN(shareOwner, "snap-private"), wantCode: "DBSnapshotNotFound",
		},
		{
			name:     "describe public by arn",
			op:       "describe",
			caller:   shareOther,
			snapshot: snapshotARN(shareOwner, "snap-public"),
		},
		{
			name: "describe shared by third party", op: "describe", caller: shareOther,
			snapshot: snapshotARN(shareOwner, "snap-shared"), wantCode: "DBSnapshotNotFound",
		},
		{name: "restore shared", op: "restore", caller: shareGuest, snapshot: snapshotARN(shareOwner, "snap-shared")},
		{name: "restore public", op: "restore", caller: shareOther, snapshot: snapshotARN(shareOwner, "snap-public")},
		{
			name: "restore private", op: "restore", caller: shareGuest,
			snapshot: snapshotARN(shareOwner, "snap-private"),
			wantCode: "DBSnapshotNotFound",
		},
		{
			name: "restore shared by third party", op: "restore", caller: shareOther,
			snapshot: snapshotARN(shareOwner, "snap-shared"), wantCode: "DBSnapshotNotFound",
		},
		{
			name: "restore shared by name", op: "restore", caller: shareGuest, snapshot: "snap-shared",
			wantCode: "DBSnapshotNotFound",
		},
		{name: "restore own", op: "restore", caller: shareOwner, snapshot: "snap-private"},
		{name: "copy shared", op: "copy", caller: shareGuest, snapshot: snapshotARN(shareOwner, "snap-shared")},
		{
			name: "copy private", op: "copy", caller: shareGuest, snapshot: snapshotARN(shareOwner, "snap-private"),
			wantCode: "DBSnapshotNotFound",
		},
		{
			name: "modify attribute as guest", op: "modify", caller: shareGuest, snapshot: "snap-shared",
			wantCode: "DBSnapshotNotFound",
		},
		{
			name: "describe attributes as guest", op: "attrs", caller: shareGuest, snapshot: "snap-shared",
			wantCode: "DBSnapshotNotFound",
		},
		{
			name:     "delete as guest",
			op:       "delete",
			caller:   shareGuest,
			snapshot: "snap-shared",
			wantCode: "DBSnapshotNotFound",
		},
		{name: "delete as owner", op: "delete", caller: shareOwner, snapshot: "snap-shared"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newShareStack(t)
			s.seedSnapshots(t)
			c := s.clients[tt.caller]

			var err error

			switch tt.op {
			case "describe":
				_, err = c.DescribeDBSnapshots(t.Context(), &rdssdk.DescribeDBSnapshotsInput{
					DBSnapshotIdentifier: aws.String(tt.snapshot),
				})
			case "restore":
				_, err = c.RestoreDBInstanceFromDBSnapshot(t.Context(), &rdssdk.RestoreDBInstanceFromDBSnapshotInput{
					DBInstanceIdentifier: aws.String("restored"), DBSnapshotIdentifier: aws.String(tt.snapshot),
				})
			case "copy":
				_, err = c.CopyDBSnapshot(t.Context(), &rdssdk.CopyDBSnapshotInput{
					SourceDBSnapshotIdentifier: aws.String(tt.snapshot), TargetDBSnapshotIdentifier: aws.String("copy"),
				})
			case "modify":
				_, err = c.ModifyDBSnapshotAttribute(t.Context(), &rdssdk.ModifyDBSnapshotAttributeInput{
					DBSnapshotIdentifier: aws.String(tt.snapshot), AttributeName: aws.String("restore"),
					ValuesToAdd: []string{shareOther},
				})
			case "attrs":
				_, err = c.DescribeDBSnapshotAttributes(t.Context(), &rdssdk.DescribeDBSnapshotAttributesInput{
					DBSnapshotIdentifier: aws.String(tt.snapshot),
				})
			default:
				_, err = c.DeleteDBSnapshot(
					t.Context(),
					&rdssdk.DeleteDBSnapshotInput{DBSnapshotIdentifier: aws.String(tt.snapshot)},
				)
			}

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantCode)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDBSnapshotCopyOwnedByCaller(t *testing.T) {
	t.Parallel()

	s := newShareStack(t)
	s.seedSnapshots(t)

	out, err := s.clients[shareGuest].CopyDBSnapshot(t.Context(), &rdssdk.CopyDBSnapshotInput{
		SourceDBSnapshotIdentifier: aws.String(snapshotARN(shareOwner, "snap-shared")),
		TargetDBSnapshotIdentifier: aws.String("guest-copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, snapshotARN(shareGuest, "guest-copy"), aws.ToString(out.DBSnapshot.DBSnapshotArn))

	guest, err := s.clients[shareGuest].DescribeDBSnapshots(t.Context(), &rdssdk.DescribeDBSnapshotsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"guest-copy"}, snapshotIDs(guest))

	owner, err := s.clients[shareOwner].DescribeDBSnapshots(t.Context(), &rdssdk.DescribeDBSnapshotsInput{})
	require.NoError(t, err)
	assert.NotContains(t, snapshotIDs(owner), "guest-copy")
}

func TestUnshareRevokesAccess(t *testing.T) {
	t.Parallel()

	s := newShareStack(t)
	s.seedSnapshots(t)

	_, err := s.clients[shareOwner].ModifyDBSnapshotAttribute(t.Context(), &rdssdk.ModifyDBSnapshotAttributeInput{
		DBSnapshotIdentifier: aws.String("snap-shared"), AttributeName: aws.String("restore"),
		ValuesToRemove: []string{shareGuest},
	})
	require.NoError(t, err)

	out, err := s.clients[shareGuest].DescribeDBSnapshots(t.Context(), &rdssdk.DescribeDBSnapshotsInput{
		IncludeShared: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Empty(t, out.DBSnapshots)
}

func TestSharedDBClusterSnapshots(t *testing.T) {
	t.Parallel()

	s := newShareStack(t)
	owner := s.clients[shareOwner]
	ctx := t.Context()

	_, err := owner.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(
			"csrc",
		), Engine: aws.String("aurora-mysql"), MasterUsername: aws.String("admin"),
		MasterUserPassword: aws.String("Passw0rd!x"),
	})
	require.NoError(t, err)

	for _, id := range []string{"csnap-shared", "csnap-private"} {
		_, err = owner.CreateDBClusterSnapshot(ctx, &rdssdk.CreateDBClusterSnapshotInput{
			DBClusterIdentifier: aws.String("csrc"), DBClusterSnapshotIdentifier: aws.String(id),
		})
		require.NoError(t, err)
	}

	_, err = owner.ModifyDBClusterSnapshotAttribute(ctx, &rdssdk.ModifyDBClusterSnapshotAttributeInput{
		DBClusterSnapshotIdentifier: aws.String("csnap-shared"), AttributeName: aws.String("restore"),
		ValuesToAdd: []string{shareGuest},
	})
	require.NoError(t, err)

	guest := s.clients[shareGuest]

	def, err := guest.DescribeDBClusterSnapshots(ctx, &rdssdk.DescribeDBClusterSnapshotsInput{})
	require.NoError(t, err)
	assert.Empty(t, def.DBClusterSnapshots)

	shared, err := guest.DescribeDBClusterSnapshots(
		ctx,
		&rdssdk.DescribeDBClusterSnapshotsInput{IncludeShared: aws.Bool(true)},
	)
	require.NoError(t, err)
	require.Len(t, shared.DBClusterSnapshots, 1)
	assert.Equal(t, "csnap-shared", aws.ToString(shared.DBClusterSnapshots[0].DBClusterSnapshotIdentifier))

	byType, err := s.clients[shareOther].DescribeDBClusterSnapshots(ctx, &rdssdk.DescribeDBClusterSnapshotsInput{
		SnapshotType: aws.String("shared"),
	})
	require.NoError(t, err)
	assert.Empty(t, byType.DBClusterSnapshots)

	tests := []struct {
		name     string
		caller   string
		snapshot string
		wantCode string
	}{
		{name: "restore shared", caller: shareGuest, snapshot: clusterSnapshotARN(shareOwner, "csnap-shared")},
		{
			name: "restore private", caller: shareGuest, snapshot: clusterSnapshotARN(shareOwner, "csnap-private"),
			wantCode: "DBClusterSnapshotNotFound",
		},
		{
			name: "restore by name", caller: shareGuest, snapshot: "csnap-shared",
			wantCode: "DBClusterSnapshotNotFound",
		},
		{
			name: "restore third party", caller: shareOther, snapshot: clusterSnapshotARN(shareOwner, "csnap-shared"),
			wantCode: "DBClusterSnapshotNotFound",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, rerr := s.clients[tt.caller].RestoreDBClusterFromSnapshot(ctx, &rdssdk.RestoreDBClusterFromSnapshotInput{
				DBClusterIdentifier: aws.String("restored-" + strconv.Itoa(i)),
				SnapshotIdentifier:  aws.String(tt.snapshot), Engine: aws.String("aurora-mysql"),
			})
			if tt.wantCode != "" {
				require.Error(t, rerr)
				assert.Contains(t, rerr.Error(), tt.wantCode)

				return
			}

			require.NoError(t, rerr)
		})
	}
}
