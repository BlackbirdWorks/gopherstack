package ec2_test

import (
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	shOwner = "111111111111"
	shGuest = "222222222222"
	shOther = "333333333333"
)

type sharingStack struct {
	clients   map[string]*ec2sdk.Client
	snapshots map[string]string
	images    map[string]string
}

func newSharingStack(t *testing.T) *sharingStack {
	t.Helper()

	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			c.SetRequest(req.WithContext(awsmeta.Set(req.Context(), awsmeta.FromRequest(req, "us-east-1"))))

			return next(c)
		}
	})

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	s := &sharingStack{
		clients: map[string]*ec2sdk.Client{}, snapshots: map[string]string{}, images: map[string]string{},
	}

	for _, acct := range []string{shOwner, shGuest, shOther} {
		cfg, err := awscfg.LoadDefaultConfig(t.Context(),
			awscfg.WithRegion("us-east-1"),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acct, "x", "")),
		)
		require.NoError(t, err)

		s.clients[acct] = ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	}

	s.seed(t)

	return s
}

func (s *sharingStack) seed(t *testing.T) {
	t.Helper()

	owner := s.clients[shOwner]
	ctx := t.Context()

	vol, err := owner.CreateVolume(
		ctx,
		&ec2sdk.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8)},
	)
	require.NoError(t, err)

	for _, name := range []string{"shared", "public", "private"} {
		snap, serr := owner.CreateSnapshot(ctx, &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
		require.NoError(t, serr)
		assert.Equal(t, shOwner, aws.ToString(snap.OwnerId))
		s.snapshots[name] = aws.ToString(snap.SnapshotId)

		img, ierr := owner.RegisterImage(ctx, &ec2sdk.RegisterImageInput{
			Name: aws.String("img-" + name), Architecture: types.ArchitectureValuesX8664,
		})
		require.NoError(t, ierr)
		s.images[name] = aws.ToString(img.ImageId)
	}

	_, err = owner.ModifySnapshotAttribute(ctx, &ec2sdk.ModifySnapshotAttributeInput{
		SnapshotId: aws.String(s.snapshots["shared"]), Attribute: types.SnapshotAttributeNameCreateVolumePermission,
		CreateVolumePermission: &types.CreateVolumePermissionModifications{
			Add: []types.CreateVolumePermission{{UserId: aws.String(shGuest)}},
		},
	})
	require.NoError(t, err)

	_, err = owner.ModifySnapshotAttribute(ctx, &ec2sdk.ModifySnapshotAttributeInput{
		SnapshotId: aws.String(s.snapshots["public"]), Attribute: types.SnapshotAttributeNameCreateVolumePermission,
		CreateVolumePermission: &types.CreateVolumePermissionModifications{
			Add: []types.CreateVolumePermission{{Group: types.PermissionGroupAll}},
		},
	})
	require.NoError(t, err)

	_, err = owner.ModifyImageAttribute(ctx, &ec2sdk.ModifyImageAttributeInput{
		ImageId: aws.String(s.images["shared"]), Attribute: aws.String("launchPermission"),
		LaunchPermission: &types.LaunchPermissionModifications{
			Add: []types.LaunchPermission{{UserId: aws.String(shGuest)}},
		},
	})
	require.NoError(t, err)

	_, err = owner.ModifyImageAttribute(ctx, &ec2sdk.ModifyImageAttributeInput{
		ImageId: aws.String(s.images["public"]), Attribute: aws.String("launchPermission"),
		LaunchPermission: &types.LaunchPermissionModifications{
			Add: []types.LaunchPermission{{Group: types.PermissionGroupAll}},
		},
	})
	require.NoError(t, err)
}

func (s *sharingStack) names(ids map[string]string, got []string) []string {
	out := make([]string, 0, len(got))

	for name, id := range ids {
		if slices.Contains(got, id) {
			out = append(out, name)
		}
	}

	slices.Sort(out)

	return out
}

func requireEC2Code(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), code)
}

func TestDescribeSnapshotsSharing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		caller       string
		owners       []string
		restorableBy []string
		want         []string
	}{
		{name: "owner default", caller: shOwner, want: []string{"private", "public", "shared"}},
		{name: "guest default", caller: shGuest, want: []string{"public", "shared"}},
		{name: "other default", caller: shOther, want: []string{"public"}},
		{name: "owner self", caller: shOwner, owners: []string{"self"}, want: []string{"private", "public", "shared"}},
		{name: "guest self", caller: shGuest, owners: []string{"self"}, want: []string{}},
		{name: "guest by owner id", caller: shGuest, owners: []string{shOwner}, want: []string{"public", "shared"}},
		{name: "guest restorable self", caller: shGuest, restorableBy: []string{"self"}, want: []string{"shared"}},
		{name: "owner restorable guest", caller: shOwner, restorableBy: []string{shGuest}, want: []string{"shared"}},
		{name: "other restorable all", caller: shOther, restorableBy: []string{"all"}, want: []string{"public"}},
		{name: "other restorable guest", caller: shOther, restorableBy: []string{shGuest}, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSharingStack(t)

			out, err := s.clients[tt.caller].DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
				OwnerIds: tt.owners, RestorableByUserIds: tt.restorableBy,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Snapshots))
			for _, sn := range out.Snapshots {
				got = append(got, aws.ToString(sn.SnapshotId))
			}

			assert.Equal(t, tt.want, s.names(s.snapshots, got))
		})
	}
}

func TestSharedSnapshotAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		caller   string
		snapshot string
		wantCode string
	}{
		{name: "describe shared as guest", op: "describe", caller: shGuest, snapshot: "shared"},
		{name: "describe private as guest", op: "describe", caller: shGuest, snapshot: "private", wantCode: "NotFound"},
		{name: "describe shared as other", op: "describe", caller: shOther, snapshot: "shared", wantCode: "NotFound"},
		{name: "volume from shared", op: "volume", caller: shGuest, snapshot: "shared"},
		{name: "volume from public", op: "volume", caller: shOther, snapshot: "public"},
		{name: "volume from private", op: "volume", caller: shGuest, snapshot: "private", wantCode: "NotFound"},
		{name: "volume from shared as other", op: "volume", caller: shOther, snapshot: "shared", wantCode: "NotFound"},
		{name: "volume from private as owner", op: "volume", caller: shOwner, snapshot: "private"},
		{name: "copy shared", op: "copy", caller: shGuest, snapshot: "shared"},
		{name: "copy private", op: "copy", caller: shGuest, snapshot: "private", wantCode: "NotFound"},
		{name: "modify as guest", op: "modify", caller: shGuest, snapshot: "shared", wantCode: "UnauthorizedOperation"},
		{name: "modify as other", op: "modify", caller: shOther, snapshot: "shared", wantCode: "NotFound"},
		{name: "modify as owner", op: "modify", caller: shOwner, snapshot: "shared"},
		{
			name:     "attribute as guest",
			op:       "attribute",
			caller:   shGuest,
			snapshot: "shared",
			wantCode: "UnauthorizedOperation",
		},
		{name: "attribute as owner", op: "attribute", caller: shOwner, snapshot: "shared"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSharingStack(t)
			c := s.clients[tt.caller]
			id := aws.String(s.snapshots[tt.snapshot])

			var err error

			switch tt.op {
			case "describe":
				_, err = c.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{SnapshotIds: []string{*id}})
			case "volume":
				_, err = c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
					AvailabilityZone: aws.String("us-east-1a"), SnapshotId: id,
				})
			case "copy":
				_, err = c.CopySnapshot(t.Context(), &ec2sdk.CopySnapshotInput{
					SourceSnapshotId: id, SourceRegion: aws.String("us-east-1"),
				})
			case "modify":
				_, err = c.ModifySnapshotAttribute(t.Context(), &ec2sdk.ModifySnapshotAttributeInput{
					SnapshotId: id, Attribute: types.SnapshotAttributeNameCreateVolumePermission,
					CreateVolumePermission: &types.CreateVolumePermissionModifications{
						Add: []types.CreateVolumePermission{{UserId: aws.String(shOther)}},
					},
				})
			default:
				_, err = c.DescribeSnapshotAttribute(t.Context(), &ec2sdk.DescribeSnapshotAttributeInput{
					SnapshotId: id, Attribute: types.SnapshotAttributeNameCreateVolumePermission,
				})
			}

			if tt.wantCode != "" {
				requireEC2Code(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSnapshotCopyOwnedByCallerAndUnshare(t *testing.T) {
	t.Parallel()

	s := newSharingStack(t)
	ctx := t.Context()

	cp, err := s.clients[shGuest].CopySnapshot(ctx, &ec2sdk.CopySnapshotInput{
		SourceSnapshotId: aws.String(s.snapshots["shared"]), SourceRegion: aws.String("us-east-1"),
	})
	require.NoError(t, err)

	mine, err := s.clients[shGuest].DescribeSnapshots(ctx, &ec2sdk.DescribeSnapshotsInput{OwnerIds: []string{"self"}})
	require.NoError(t, err)
	require.Len(t, mine.Snapshots, 1)
	assert.Equal(t, aws.ToString(cp.SnapshotId), aws.ToString(mine.Snapshots[0].SnapshotId))
	assert.Equal(t, shGuest, aws.ToString(mine.Snapshots[0].OwnerId))

	_, err = s.clients[shOther].DescribeSnapshots(ctx, &ec2sdk.DescribeSnapshotsInput{
		SnapshotIds: []string{aws.ToString(cp.SnapshotId)},
	})
	requireEC2Code(t, err, "NotFound")

	_, err = s.clients[shOwner].ModifySnapshotAttribute(ctx, &ec2sdk.ModifySnapshotAttributeInput{
		SnapshotId: aws.String(s.snapshots["shared"]), Attribute: types.SnapshotAttributeNameCreateVolumePermission,
		CreateVolumePermission: &types.CreateVolumePermissionModifications{
			Remove: []types.CreateVolumePermission{{UserId: aws.String(shGuest)}},
		},
	})
	require.NoError(t, err)

	_, err = s.clients[shGuest].CreateVolume(ctx, &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"), SnapshotId: aws.String(s.snapshots["shared"]),
	})
	requireEC2Code(t, err, "NotFound")
}

func TestDescribeImagesSharing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		caller     string
		owners     []string
		executable []string
		want       []string
	}{
		{
			name:   "owner by self",
			caller: shOwner,
			owners: []string{"self"},
			want:   []string{"private", "public", "shared"},
		},
		{name: "guest by owner", caller: shGuest, owners: []string{shOwner}, want: []string{"public", "shared"}},
		{name: "other by owner", caller: shOther, owners: []string{shOwner}, want: []string{"public"}},
		{name: "guest self", caller: shGuest, owners: []string{"self"}, want: []string{}},
		{
			name: "guest executable self", caller: shGuest, owners: []string{shOwner}, executable: []string{"self"},
			want: []string{"shared"},
		},
		{
			name: "other executable guest", caller: shOther, owners: []string{shOwner}, executable: []string{shGuest},
			want: []string{},
		},
		{
			name: "owner executable all", caller: shOwner, owners: []string{shOwner}, executable: []string{"all"},
			want: []string{"public"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSharingStack(t)

			out, err := s.clients[tt.caller].DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
				Owners: tt.owners, ExecutableUsers: tt.executable,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Images))
			for _, im := range out.Images {
				got = append(got, aws.ToString(im.ImageId))
			}

			assert.Equal(t, tt.want, s.names(s.images, got))
		})
	}
}

func TestSharedImageAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		caller   string
		image    string
		wantCode string
	}{
		{name: "describe shared as guest", op: "describe", caller: shGuest, image: "shared"},
		{
			name:     "describe private as guest",
			op:       "describe",
			caller:   shGuest,
			image:    "private",
			wantCode: "InvalidAMIID.NotFound",
		},
		{
			name:     "describe shared as other",
			op:       "describe",
			caller:   shOther,
			image:    "shared",
			wantCode: "InvalidAMIID.NotFound",
		},
		{name: "run shared", op: "run", caller: shGuest, image: "shared"},
		{name: "run public", op: "run", caller: shOther, image: "public"},
		{name: "run private", op: "run", caller: shGuest, image: "private", wantCode: "InvalidAMIID.NotFound"},
		{name: "run shared as other", op: "run", caller: shOther, image: "shared", wantCode: "InvalidAMIID.NotFound"},
		{name: "run private as owner", op: "run", caller: shOwner, image: "private"},
		{name: "copy shared", op: "copy", caller: shGuest, image: "shared"},
		{name: "copy private", op: "copy", caller: shGuest, image: "private", wantCode: "InvalidAMIID.NotFound"},
		{name: "modify as guest", op: "modify", caller: shGuest, image: "shared", wantCode: "UnauthorizedOperation"},
		{name: "modify as other", op: "modify", caller: shOther, image: "shared", wantCode: "InvalidAMIID.NotFound"},
		{name: "modify as owner", op: "modify", caller: shOwner, image: "shared"},
		{
			name:     "attribute as guest",
			op:       "attribute",
			caller:   shGuest,
			image:    "shared",
			wantCode: "UnauthorizedOperation",
		},
		{name: "attribute as owner", op: "attribute", caller: shOwner, image: "shared"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSharingStack(t)
			c := s.clients[tt.caller]
			id := aws.String(s.images[tt.image])

			var err error

			switch tt.op {
			case "describe":
				_, err = c.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{ImageIds: []string{*id}})
			case "run":
				_, err = c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
					ImageId: id, InstanceType: types.InstanceTypeT3Micro,
					MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
				})
			case "copy":
				_, err = c.CopyImage(t.Context(), &ec2sdk.CopyImageInput{
					SourceImageId: id, SourceRegion: aws.String("us-east-1"), Name: aws.String("copy"),
				})
			case "modify":
				_, err = c.ModifyImageAttribute(t.Context(), &ec2sdk.ModifyImageAttributeInput{
					ImageId: id, Attribute: aws.String("launchPermission"),
					LaunchPermission: &types.LaunchPermissionModifications{
						Add: []types.LaunchPermission{{UserId: aws.String(shOther)}},
					},
				})
			default:
				_, err = c.DescribeImageAttribute(t.Context(), &ec2sdk.DescribeImageAttributeInput{
					ImageId: id, Attribute: types.ImageAttributeNameLaunchPermission,
				})
			}

			if tt.wantCode != "" {
				requireEC2Code(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)
		})
	}
}
