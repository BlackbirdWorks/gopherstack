package ociidentity_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestValidateStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stmt    string
		wantErr bool
	}{
		{"manage compartment", "Allow group A-Admins to manage all-resources in compartment Project-A", false},
		{"group list", "Allow group A-Admins, B-Admins to manage all-resources in compartment Projects-A-and-B", false},
		{"any-group tenancy", "Allow any-group to inspect users in tenancy", false},
		{
			"nested path",
			"Allow group InstanceAdmins to manage instance-family in compartment Project-A:Project-A2",
			false,
		},
		{
			"where pattern",
			"Allow group GroupAdmins to manage groups in tenancy where target.group.name = /A-Users-*/",
			false,
		},
		{
			"where not equal",
			"Allow group NetworkAdmins to manage virtual-network-family in tenancy\n" +
				"  where target.compartment.id != 'ocid1.compartment.oc1..aaaa'",
			false,
		},
		{
			"where all",
			"Allow group G to manage groups in tenancy where all {target.group.name=/A-*/,target.group.name!='A-Admins'}",
			false,
		},
		{
			"group id",
			"Allow group id ocid1.group.oc1..aaaa to read buckets in compartment id ocid1.compartment.oc1..bbbb",
			false,
		},
		{"dynamic group", "Allow dynamic-group fn-dg to use keys in tenancy", false},
		{"service", "Allow service objectstorage-us-ashburn-1 to manage object-family in tenancy", false},
		{"lowercase", "allow group a to read buckets in tenancy", false},
		{"deny", "Deny group a to manage buckets in tenancy", false},
		{"define", "Define tenancy Acme as ocid1.tenancy.oc1..aaaa", false},
		{"endorse", "Endorse group Admins to read object-family in tenancy Acme", false},
		{"endorse any", "Endorse any-user to read object-family in any-tenancy", false},
		{"admit", "Admit group id ocid1.group.oc1..aaaa of tenancy Acme to read object-family in compartment X", false},
		{"permission list", "Allow group a to {BUCKET_READ, OBJECT_INSPECT} buckets in tenancy", false},
		{"in list", "Allow group a to read buckets in tenancy where request.region in ('iad','phx')", false},
		{"empty", "", true},
		{"unknown keyword", "Permit group a to read buckets in tenancy", true},
		{"bad verb", "Allow group a to destroy buckets in tenancy", true},
		{"missing location", "Allow group a to read buckets", true},
		{"missing in", "Allow group a to read buckets tenancy", true},
		{"bad subject", "Allow everyone to read buckets in tenancy", true},
		{"bad location", "Allow group a to read buckets in region iad", true},
		{"trailing", "Allow group a to read buckets in tenancy now", true},
		{"dangling where", "Allow group a to read buckets in tenancy where", true},
		{"unbalanced braces", "Allow group a to read buckets in tenancy where all {x.y = 'a'", true},
		{"unterminated quote", "Allow group a to read buckets in tenancy where x.y = 'a", true},
		{"define missing as", "Define tenancy Acme ocid1.tenancy.oc1..aaaa", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := oci.ValidateStatement(tt.stmt)
			if tt.wantErr {
				require.ErrorIs(t, err, oci.ErrGrammar)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPolicyErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	comp := e.mkCompartment(t, testTenancy, "proj")
	good := []string{"Allow group admins to manage all-resources in compartment proj"}

	create := func(name string, stmts []string) (identity.CreatePolicyResponse, error) {
		return e.client.CreatePolicy(
			ctx,
			identity.CreatePolicyRequest{
				CompartmentId: comp.Id, Name: new(name), Description: new("d"), Statements: stmts},
		)
	}

	p, err := create("pol", good)
	require.NoError(t, err)

	tooMany := make([]string, 51)
	for i := range tooMany {
		tooMany[i] = "Allow group g" + strconv.Itoa(i) + " to read buckets in tenancy"
	}

	tests := []struct {
		name     string
		call     func() error
		wantCode string
		status   int
	}{
		{"malformed statement", func() error {
			_, cerr := create("bad", []string{"Allow group admins manage all-resources in tenancy"})

			return cerr
		}, "InvalidParameter", http.StatusBadRequest},
		{
			"no statements",
			func() error {
				_, cerr := create("none", []string{})

				return cerr
			},
			"MissingParameter",
			http.StatusBadRequest,
		},
		{
			"too many statements",
			func() error {
				_, cerr := create("many", tooMany)

				return cerr
			},
			"LimitExceeded",
			http.StatusBadRequest,
		},
		{
			"duplicate name",
			func() error {
				_, cerr := create("POL", good)

				return cerr
			},
			"Conflict",
			http.StatusConflict,
		},
		{"unknown compartment", func() error {
			_, cerr := e.client.CreatePolicy(
				ctx,
				identity.CreatePolicyRequest{
					CompartmentId: new(
						"ocid1.compartment.oc1..zz",
					),
					Name:        new("x"),
					Description: new("d"),
					Statements:  good},
			)

			return cerr
		}, "NotAuthorizedOrNotFound", http.StatusNotFound},
		{"update bad statement", func() error {
			_, cerr := e.client.UpdatePolicy(ctx, identity.UpdatePolicyRequest{PolicyId: p.Id,
				Statements: []string{"nonsense"}})

			return cerr
		}, "InvalidParameter", http.StatusBadRequest},
		{"delete compartment with policy", func() error {
			_, cerr := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: comp.Id})

			return cerr
		}, "Conflict", http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestPolicies(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	comp := e.mkCompartment(t, testTenancy, "proj")
	good := []string{"Allow group admins to manage all-resources in compartment proj"}

	create := func(name string, stmts []string, ver *common.SDKDate) (identity.CreatePolicyResponse, error) {
		return e.client.CreatePolicy(
			ctx,
			identity.CreatePolicyRequest{
				CompartmentId: comp.Id, Name: new(name), Description: new("d"), Statements: stmts, VersionDate: ver},
		)
	}

	date := &common.SDKDate{Date: mustDate(t, "2024-01-31")}

	p, err := create("pol", good, date)
	require.NoError(t, err)
	assert.Equal(t, good, p.Statements)
	require.NotNil(t, p.VersionDate)
	assert.Equal(t, "2024-01-31", p.VersionDate.Date.Format("2006-01-02"))

	up, err := e.client.UpdatePolicy(ctx, identity.UpdatePolicyRequest{PolicyId: p.Id, IfMatch: p.Etag,
		Statements: []string{
			"Allow group a to read buckets in tenancy",
			"Allow group b to read buckets in tenancy",
		}})
	require.NoError(t, err)
	assert.Len(t, up.Statements, 2)

	lst, err := e.client.ListPolicies(ctx, identity.ListPoliciesRequest{CompartmentId: comp.Id})
	require.NoError(t, err)
	assert.Len(t, lst.Items, 1)

	other, err := e.client.ListPolicies(ctx, identity.ListPoliciesRequest{CompartmentId: new(testTenancy)})
	require.NoError(t, err)
	assert.Empty(t, other.Items)

	_, err = e.client.DeletePolicy(ctx, identity.DeletePolicyRequest{PolicyId: p.Id})
	require.NoError(t, err)

	_, err = e.client.GetPolicy(ctx, identity.GetPolicyRequest{PolicyId: p.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
