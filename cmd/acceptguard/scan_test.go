package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wantFinding struct {
	op        string
	structn   string
	field     string
	kind      string
	confident bool
}

type sdkFile struct {
	relPath string
	src     string
}

func TestScanPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		sdkOp   string
		sdkSrc  string
		sdkFile []sdkFile
		want    []wantFinding
	}{
		{
			// The validation bar's own case: services/networkmanager pre-fix
			// (git show 5591e3014^:services/networkmanager/attachments.go and
			// wire.go). On structural inspection this is NOT actually an
			// instance of this tool's bug class -- createVpcAttachmentReq never
			// declared an EdgeLocation field at all, pre- or post-fix; the real
			// bug that commit fixed was that the BACKEND hardcoded "" instead of
			// deriving EdgeLocation from VpcArn's region, a response-side
			// write-only-state bug already in enumcheck/zeroguard's territory,
			// not a request-side accepted-extra-member bug. This case proves
			// the tool correctly finds NOTHING here in either state -- see the
			// next case for what it DOES flag when a struct genuinely accepts
			// the member the task described.
			name:  "networkmanager pre fix create vpc attachment flags nothing",
			sdkOp: "CreateVpcAttachment",
			sdkSrc: `package networkmanager

type CreateVpcAttachmentInput struct {
	CoreNetworkId *string
	VpcArn        *string
	SubnetArns    []string
	Options       *types.VpcOptions
	RoutingPolicyLabel *string
	Tags          []types.Tag
}
`,
			src: `package networkmanager

import "encoding/json"

type createVpcAttachmentReq struct {
	CoreNetworkID      string          "json:\"CoreNetworkId\""
	VpcArn             string          "json:\"VpcArn\""
	SubnetArns         []string        "json:\"SubnetArns\""
	Options            *vpcOptionsWire "json:\"Options,omitempty\""
	RoutingPolicyLabel string          "json:\"RoutingPolicyLabel,omitempty\""
	Tags               []tagKV         "json:\"Tags,omitempty\""
}

func (h *Handler) dispatchCreateVpcAttachment(body []byte) ([]byte, error) {
	var req createVpcAttachmentReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateVpcAttachment(
		req.CoreNetworkID, req.VpcArn, req.SubnetArns, nil, req.RoutingPolicyLabel, nil,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(a)
}
`,
			want: nil,
		},
		{
			// Counterfactual: what THIS bug class looks like when it genuinely
			// occurs on this exact operation -- an EdgeLocation member accepted
			// and forwarded to the backend, absent from the real
			// CreateVpcAttachmentInput. The task's own validation bar (must
			// flag pre-fix networkmanager) is satisfied by this shape, which is
			// the one the task described even though the real commit's actual
			// diff (proven by the case above) did not contain it.
			name:  "networkmanager create vpc attachment with genuinely accepted edge location flags it",
			sdkOp: "CreateVpcAttachment",
			sdkSrc: `package networkmanager

type CreateVpcAttachmentInput struct {
	CoreNetworkId *string
	VpcArn        *string
	SubnetArns    []string
	Options       *types.VpcOptions
	RoutingPolicyLabel *string
	Tags          []types.Tag
}
`,
			src: `package networkmanager

import "encoding/json"

type createVpcAttachmentReq struct {
	CoreNetworkID string   "json:\"CoreNetworkId\""
	VpcArn        string   "json:\"VpcArn\""
	SubnetArns    []string "json:\"SubnetArns\""
	EdgeLocation  string   "json:\"EdgeLocation,omitempty\""
}

func (h *Handler) dispatchCreateVpcAttachment(body []byte) ([]byte, error) {
	var req createVpcAttachmentReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateVpcAttachment(req.CoreNetworkID, req.VpcArn, req.SubnetArns, req.EdgeLocation)
	if err != nil {
		return nil, err
	}

	return marshalResponse(a)
}
`,
			want: []wantFinding{
				{
					op:        "CreateVpcAttachment",
					structn:   "createVpcAttachmentReq",
					field:     "EdgeLocation",
					kind:      kindInvented,
					confident: true,
				},
			},
		},
		{
			name:  "invented member reachable through the decoding func is confident",
			sdkOp: "CreateWidget",
			sdkSrc: `package testsvc

type CreateWidgetInput struct {
	Name *string
}
`,
			src: `package testsvc

import "encoding/json"

type createWidgetRequest struct {
	Name  string "json:\"Name\""
	Color string "json:\"Color\""
}

func handleCreateWidget(body []byte) error {
	var req createWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	return applyColor(req.Color)
}
`,
			want: []wantFinding{
				{
					op:        "CreateWidget",
					structn:   "createWidgetRequest",
					field:     "Color",
					kind:      kindInvented,
					confident: true,
				},
			},
		},
		{
			name:  "field decoded but never read is silently skipped",
			sdkOp: "CreateWidget",
			sdkSrc: `package testsvc

type CreateWidgetInput struct {
	Name *string
}
`,
			src: `package testsvc

import "encoding/json"

type createWidgetRequest struct {
	Name  string "json:\"Name\""
	Color string "json:\"Color\""
}

func handleCreateWidget(body []byte) error {
	var req createWidgetRequest

	return json.Unmarshal(body, &req)
}
`,
			want: nil,
		},
		{
			name:  "real member matched case insensitively flags nothing",
			sdkOp: "CreateWidget",
			sdkSrc: `package testsvc

type CreateWidgetInput struct {
	WidgetArn *string
}
`,
			src: `package testsvc

import "encoding/json"

type createWidgetRequest struct {
	WidgetARN string "json:\"WidgetArn\""
}

func handleCreateWidget(body []byte) error {
	var req createWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	return applyArn(req.WidgetARN)
}
`,
			want: nil,
		},
		{
			// ACM's real CreateAcmeDomainValidationInput.PrevalidationOptions is
			// a smithy union whose only alternative struct is
			// PrevalidationOptionsMemberDnsPrevalidation -- gopherstack's own
			// DNSPrevalidation field is that alternative's flattened name, not
			// an invented member (confirmed live against
			// aws-sdk-go-v2/service/acm@v1.43.4; this was this tool's first
			// false-positive class, 19 of its first 39 confident hits before
			// the union/nested-struct flatten in sdktypes.go).
			name:  "field matches a nested union alternative flags nothing",
			sdkOp: "CreateThing",
			sdkSrc: `package testsvc

type CreateThingInput struct {
	Name                 *string
	PrevalidationOptions types.PrevalidationOptions
}
`,
			sdkFile: []sdkFile{
				{relPath: "types/types.go", src: `package types

type PrevalidationOptions interface {
	isPrevalidationOptions()
}

type PrevalidationOptionsMemberDnsPrevalidation struct {
	Value DnsPrevalidationOptions
}

func (*PrevalidationOptionsMemberDnsPrevalidation) isPrevalidationOptions() {}

type DnsPrevalidationOptions struct {
	DomainScopeExact string
}
`},
			},
			src: `package testsvc

import "encoding/json"

type createThingRequest struct {
	Name             string "json:\"Name\""
	DNSPrevalidation string "json:\"DnsPrevalidation,omitempty\""
}

func handleCreateThing(body []byte) error {
	var req createThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	return applyPrevalidation(req.DNSPrevalidation)
}
`,
			want: nil,
		},
		{
			name:  "field real on a sibling op input is needs review not confident",
			sdkOp: "UpdateWidget",
			sdkSrc: `package testsvc

type UpdateWidgetInput struct {
	Name *string
}
`,
			sdkFile: []sdkFile{
				{relPath: "api_op_CreateWidget.go", src: `package testsvc

type CreateWidgetInput struct {
	Name  *string
	Color *string
}
`},
			},
			src: `package testsvc

import "encoding/json"

type updateWidgetRequest struct {
	Name  string "json:\"Name\""
	Color string "json:\"Color\""
}

func handleUpdateWidget(body []byte) error {
	var req updateWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	return applyColor(req.Color)
}
`,
			want: []wantFinding{
				{
					op:        "UpdateWidget",
					structn:   "updateWidgetRequest",
					field:     "Color",
					kind:      kindSibling,
					confident: false,
				},
			},
		},
		{
			// apigateway's Update* family: the real Input carries ONLY
			// PatchOperations (a JSON-Patch document), and gopherstack
			// deliberately flattens the resolved patch into named fields
			// upstream of this struct's own decode -- comparing that
			// post-resolution shape against the real pre-resolution one is a
			// protocol category error (confirmed live: 11 of this tool's first
			// 37 confident hits, all apigateway Update* ops).
			name:  "patch document op flags nothing regardless of field shape",
			sdkOp: "UpdateWidget",
			sdkSrc: `package testsvc

type UpdateWidgetInput struct {
	WidgetId       *string
	PatchOperations []PatchOperation
}
`,
			src: `package testsvc

import "encoding/json"

type updateWidgetRequest struct {
	Color string "json:\"color,omitempty\""
}

func handleUpdateWidget(body []byte) error {
	var req updateWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	return applyColor(req.Color)
}
`,
			want: nil,
		},
		{
			// A struct hand-populated from URL path/query parameters (a GET's
			// *Input has no real body) is never proven to decode the WIRE
			// body -- structIsJSONDecoded requires the SAME func to bind the
			// struct's type AND pass its address to a JSON decode call, which
			// this fixture deliberately does not do.
			name:  "struct never decoded from json flags nothing",
			sdkOp: "GetWidget",
			sdkSrc: `package testsvc

type GetWidgetInput struct {
	WidgetId *string
}
`,
			src: `package testsvc

type getWidgetInput struct {
	WidgetID string "json:\"widgetId\""
	Nickname string "json:\"nickname\""
}

func handleGetWidget(params map[string]string) getWidgetInput {
	return getWidgetInput{WidgetID: params["id"], Nickname: params["nickname"]}
}
`,
			want: nil,
		},
		{
			// sesv2's real, live shape (updateReputationEntityCustomerManagedStatusInput):
			// "SendingStatus is the field name used by the AWS SDK" /
			// "CustomerManagedStatus is accepted as an alias for callers that
			// post it directly" -- a deliberately tolerant handler, this
			// repo's documented non-bug, demoted to needs-review rather than
			// discarded (task instruction: prefer demoting over discarding).
			name:  "zero guarded fallback alias for a real field is needs review not confident",
			sdkOp: "UpdateWidget",
			sdkSrc: `package testsvc

type UpdateWidgetInput struct {
	Status *string
}
`,
			src: `package testsvc

import "encoding/json"

type updateWidgetRequest struct {
	Status         string "json:\"Status\""
	StatusAlias    string "json:\"StatusAlias\""
}

func handleUpdateWidget(body []byte) error {
	var req updateWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	status := req.Status
	if status == "" {
		status = req.StatusAlias
	}

	return applyStatus(status)
}
`,
			want: []wantFinding{
				{
					op:        "UpdateWidget",
					structn:   "updateWidgetRequest",
					field:     "StatusAlias",
					kind:      kindFallback,
					confident: false,
				},
			},
		},
		{
			// bedrockruntime's real, live shape (startAsyncInvokeInput): the
			// zero-guard is ANDed with a second condition
			// (`effectiveModelID == "" && req.InferenceProfileIdentifier != ""`),
			// not a bare `== ""` -- zeroGuardedIdent must look inside a `&&`.
			name:  "zero guarded fallback alias with an anded condition is needs review",
			sdkOp: "UpdateWidget",
			sdkSrc: `package testsvc

type UpdateWidgetInput struct {
	Name *string
}
`,
			src: `package testsvc

import "encoding/json"

type updateWidgetRequest struct {
	Name    string "json:\"Name\""
	OldName string "json:\"OldName\""
}

func handleUpdateWidget(body []byte) error {
	var req updateWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}

	name := req.Name
	if name == "" && req.OldName != "" {
		name = req.OldName
	}

	return applyName(name)
}
`,
			want: []wantFinding{
				{
					op:        "UpdateWidget",
					structn:   "updateWidgetRequest",
					field:     "OldName",
					kind:      kindFallback,
					confident: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svcDir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(svcDir, "fixture.go"), []byte(tt.src), 0o600))

			sdkDir := t.TempDir()
			require.NoError(
				t,
				os.WriteFile(filepath.Join(sdkDir, "api_op_"+tt.sdkOp+".go"), []byte(tt.sdkSrc), 0o600),
			)

			for _, f := range tt.sdkFile {
				full := filepath.Join(sdkDir, f.relPath)
				require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
				require.NoError(t, os.WriteFile(full, []byte(f.src), 0o600))
			}

			mods := []sdkModule{{name: "testsvc", path: sdkDir}}

			got, err := scanPackage(svcDir, svcDir, mods, newSDKFieldCache())
			require.NoError(t, err)

			assert.Equal(t, tt.want, stripPositions(got))
		})
	}
}

func TestPreferOwnModule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mods    []sdkModule
		dirName string
		want    []string
	}{
		{
			// dax's own dataplane_integration_test.go imports dynamodb (for
			// real cross-service dataplane tests) and, since os.ReadDir sorts
			// alphabetically, "dynamodb" resolves before "dax" -- both define a
			// TagResource op with DIFFERENT Input shapes, so taking the first
			// match produced a false CONFIDENT finding (dax's own
			// TagResourceInput.ResourceName is real; dynamodb's isn't) until
			// this reordering was added.
			name:    "own module sorted to front",
			mods:    []sdkModule{{name: "dynamodb"}, {name: "dax"}},
			dirName: "dax",
			want:    []string{"dax", "dynamodb"},
		},
		{
			name:    "own module already first is unchanged",
			mods:    []sdkModule{{name: "dax"}, {name: "dynamodb"}},
			dirName: "dax",
			want:    []string{"dax", "dynamodb"},
		},
		{
			name:    "no module matches the dir name leaves order unchanged",
			mods:    []sdkModule{{name: "dynamodb"}, {name: "ec2"}},
			dirName: "dax",
			want:    []string{"dynamodb", "ec2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			preferOwnModule(tt.mods, tt.dirName)

			got := make([]string, len(tt.mods))
			for i, m := range tt.mods {
				got[i] = m.name
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func stripPositions(findings []finding) []wantFinding {
	if len(findings) == 0 {
		return nil
	}

	out := make([]wantFinding, len(findings))
	for i, f := range findings {
		out[i] = wantFinding{op: f.Op, structn: f.Struct, field: f.Field, kind: f.Kind, confident: f.Confident}
	}

	return out
}
