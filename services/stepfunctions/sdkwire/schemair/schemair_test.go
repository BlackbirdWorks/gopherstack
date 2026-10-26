package schemair_test

import (
	"testing"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/prelude"
	"github.com/aws/smithy-go/traits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire/schemair"
)

func graph() (*smithy.ServiceSchema, map[string]schemair.OpRef, []*smithy.Schema) {
	const ns = "com.example"

	id := func(n string) smithy.ShapeID { return smithy.ShapeID{Namespace: ns, Name: n} }

	name := smithy.NewSchema(id("Name"), smithy.ShapeTypeString, 0, &traits.Sensitive{})
	node := smithy.NewSchema(id("Node"), smithy.ShapeTypeStructure, 3)
	nodes := smithy.NewSchema(id("Nodes"), smithy.ShapeTypeList, 1)
	nodes.AddMember("member", node)
	node.AddMember("Name", name, &traits.JSONName{Name: "name"}, &traits.TimestampFormat{Format: "epoch-seconds"})
	node.AddMember("Kids", nodes)
	node.AddMember("Flag", prelude.Boolean, &traits.HTTPQuery{Name: "flag"})

	input := smithy.NewSchema(id("GetInput"), smithy.ShapeTypeStructure, 1)
	input.AddMember("Id", name, &traits.HTTPLabel{})
	op := smithy.NewSchema(id("Get"), smithy.ShapeTypeOperation, 0, &traits.HTTP{Method: "GET", URI: "/n/{Id}"})
	svc := smithy.NewServiceSchema(smithy.NewSchema(id("Svc"), smithy.ShapeTypeService, 0), "1")

	return svc,
		map[string]schemair.OpRef{"Get": {Op: op, In: input, Out: node, Token: "Id"}, "Void": {Op: op}},
		[]*smithy.Schema{name, node, nodes, input, op}
}

func TestEncodeBuildEquivalent(t *testing.T) {
	t.Parallel()

	svc, ops, pool := graph()

	m, err := schemair.Encode(svc, ops, pool)
	require.NoError(t, err)
	require.NoError(t, m.Check(svc, ops))
}

func TestEncodeUnknownTarget(t *testing.T) {
	t.Parallel()

	svc, ops, pool := graph()

	_, err := schemair.Encode(svc, ops, pool[:1])
	require.Error(t, err)
}

func TestEquivalentDetectsTraitDifference(t *testing.T) {
	t.Parallel()

	a := smithy.NewSchema(smithy.ShapeID{Namespace: "n", Name: "S"}, smithy.ShapeTypeStructure, 0, &traits.Sensitive{})
	b := smithy.NewSchema(smithy.ShapeID{Namespace: "n", Name: "S"}, smithy.ShapeTypeStructure, 0)

	require.NoError(t, schemair.Equivalent(a, a))
	assert.Error(t, schemair.Equivalent(a, b))
}
