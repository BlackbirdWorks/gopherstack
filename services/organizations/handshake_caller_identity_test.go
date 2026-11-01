package organizations_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

const (
	hsMgmtAcct     = "000000000000"
	hsInviteeAcct  = "111111111111"
	hsStrangerAcct = "333333333333"
)

type hsStack struct {
	clients map[string]*organizationssdk.Client
}

func newHandshakeStack(t *testing.T) *hsStack {
	t.Helper()

	principals := map[string]*awsmeta.Principal{
		"mgmt": {Kind: awsmeta.PrincipalKindUser, AccountID: hsMgmtAcct, Arn: "arn:aws:iam::000000000000:user/m"},
		"invitee": {
			Kind:      awsmeta.PrincipalKindUser,
			AccountID: hsInviteeAcct,
			Arn:       "arn:aws:iam::111111111111:user/i",
		},
		"stranger": {
			Kind:      awsmeta.PrincipalKindUser,
			AccountID: hsStrangerAcct,
			Arn:       "arn:aws:iam::333333333333:user/s",
		},
		"anon": nil,
	}

	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			meta := awsmeta.FromRequest(req, tagsRTRegion)
			if p := principals[meta.AccessKeyID]; p != nil {
				meta.Principal = p
				meta.Account = p.AccountID
			}

			c.SetRequest(req.WithContext(awsmeta.Set(req.Context(), meta)))

			return next(c)
		}
	})

	registry := service.NewRegistry()
	require.NoError(
		t,
		registry.Register(organizations.NewHandler(organizations.NewInMemoryBackend(hsMgmtAcct, tagsRTRegion))),
	)
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	s := &hsStack{clients: map[string]*organizationssdk.Client{}}

	for name := range principals {
		cfg, err := awscfg.LoadDefaultConfig(t.Context(),
			awscfg.WithRegion(tagsRTRegion),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(name, "x", "")),
		)
		require.NoError(t, err)

		s.clients[name] = organizationssdk.NewFromConfig(cfg, func(o *organizationssdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
		})
	}

	_, err := s.clients["mgmt"].CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{})
	require.NoError(t, err)

	return s
}

func (s *hsStack) invite(t *testing.T) string {
	t.Helper()

	out, err := s.clients["mgmt"].InviteAccountToOrganization(
		t.Context(),
		&organizationssdk.InviteAccountToOrganizationInput{
			Target: &organizationstypes.HandshakeParty{
				Id:   aws.String(hsInviteeAcct),
				Type: organizationstypes.HandshakePartyTypeAccount,
			},
		},
	)
	require.NoError(t, err)

	return aws.ToString(out.Handshake.Id)
}

func requireAPIErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
}

func (s *hsStack) respond(t *testing.T, op, caller, id string) error {
	t.Helper()

	var err error

	switch op {
	case "accept":
		_, err = s.clients[caller].AcceptHandshake(
			t.Context(),
			&organizationssdk.AcceptHandshakeInput{HandshakeId: aws.String(id)},
		)
	case "decline":
		_, err = s.clients[caller].DeclineHandshake(
			t.Context(),
			&organizationssdk.DeclineHandshakeInput{HandshakeId: aws.String(id)},
		)
	default:
		_, err = s.clients[caller].CancelHandshake(
			t.Context(),
			&organizationssdk.CancelHandshakeInput{HandshakeId: aws.String(id)},
		)
	}

	return err
}

func TestHandshakeCallerIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		caller   string
		wantCode string
	}{
		{name: "accept by target", op: "accept", caller: "invitee"},
		{name: "accept by originator", op: "accept", caller: "mgmt", wantCode: "AccessDeniedException"},
		{name: "accept by stranger", op: "accept", caller: "stranger", wantCode: "AccessDeniedException"},
		{name: "accept anonymous", op: "accept", caller: "anon"},
		{name: "decline by target", op: "decline", caller: "invitee"},
		{name: "decline by originator", op: "decline", caller: "mgmt", wantCode: "AccessDeniedException"},
		{name: "decline by stranger", op: "decline", caller: "stranger", wantCode: "AccessDeniedException"},
		{name: "cancel by originator", op: "cancel", caller: "mgmt"},
		{name: "cancel by target", op: "cancel", caller: "invitee", wantCode: "AccessDeniedException"},
		{name: "cancel by stranger", op: "cancel", caller: "stranger", wantCode: "AccessDeniedException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newHandshakeStack(t)
			id := s.invite(t)

			err := s.respond(t, tt.op, tt.caller, id)
			if tt.wantCode != "" {
				requireAPIErrorCode(t, err, tt.wantCode)

				got, derr := s.clients["mgmt"].DescribeHandshake(
					t.Context(),
					&organizationssdk.DescribeHandshakeInput{HandshakeId: aws.String(id)},
				)
				require.NoError(t, derr)
				assert.Equal(t, organizationstypes.HandshakeStateOpen, got.Handshake.State)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestHandshakeTransitionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		firstOp   string
		firstBy   string
		secondOp  string
		secondBy  string
		wantCode  string
		unknownID bool
	}{
		{
			name: "accept twice", firstOp: "accept", firstBy: "invitee", secondOp: "accept", secondBy: "invitee",
			wantCode: "HandshakeAlreadyInStateException",
		},
		{
			name: "decline after accept", firstOp: "accept", firstBy: "invitee",
			secondOp: "decline", secondBy: "invitee",
			wantCode: "InvalidHandshakeTransitionException",
		},
		{
			name: "cancel after accept", firstOp: "accept", firstBy: "invitee", secondOp: "cancel", secondBy: "mgmt",
			wantCode: "InvalidHandshakeTransitionException",
		},
		{
			name: "accept after cancel", firstOp: "cancel", firstBy: "mgmt", secondOp: "accept", secondBy: "invitee",
			wantCode: "InvalidHandshakeTransitionException",
		},
		{
			name: "cancel twice", firstOp: "cancel", firstBy: "mgmt", secondOp: "cancel", secondBy: "mgmt",
			wantCode: "HandshakeAlreadyInStateException",
		},
		{
			name:      "unknown handshake",
			secondOp:  "accept",
			secondBy:  "invitee",
			wantCode:  "HandshakeNotFoundException",
			unknownID: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newHandshakeStack(t)
			id := s.invite(t)

			if tt.unknownID {
				id = "h-doesnotexist"
			} else {
				require.NoError(t, s.respond(t, tt.firstOp, tt.firstBy, id))
			}

			requireAPIErrorCode(t, s.respond(t, tt.secondOp, tt.secondBy, id), tt.wantCode)
		})
	}
}

func TestEnableAllFeaturesHandshakeCaller(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		caller   string
		wantCode string
	}{
		{name: "member accepts", caller: "invitee"},
		{name: "management cannot accept", caller: "mgmt", wantCode: "AccessDeniedException"},
		{name: "non member cannot accept", caller: "stranger", wantCode: "AccessDeniedException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newHandshakeStack(t)
			require.NoError(t, s.respond(t, "accept", "invitee", s.invite(t)))

			out, err := s.clients["mgmt"].EnableAllFeatures(t.Context(), &organizationssdk.EnableAllFeaturesInput{})
			require.NoError(t, err)

			err = s.respond(t, "accept", tt.caller, aws.ToString(out.Handshake.Id))
			if tt.wantCode != "" {
				requireAPIErrorCode(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)
		})
	}
}
