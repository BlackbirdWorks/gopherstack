package azurearm

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"software.sslmate.com/src/go-pkcs12"
)

func apimTestPFX(t *testing.T, cn string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pfx, err := pkcs12.Modern.Encode(key, cert, nil, "pw")
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(pfx)
}

func TestAPIMAuthCertificate_ExpressionBodyKeysOnValue(t *testing.T) {
	t.Parallel()

	p := NewAPIMProvider()
	node := &polNode{Attrs: map[string]string{
		apimCBodyAttr: `@((string)context.Variables["pfx"])`, apimCPasswordAttr: "pw",
	}}

	clientFor := func(pfx string) *gwState {
		g := &gwState{
			p: p, plan: &gwPlan{svcKey: "svc", now: time.Now}, vars: map[string]any{"pfx": pfx},
		}
		g.authCert(node)
		require.False(t, g.errored)
		require.NotNil(t, g.client)

		return g
	}

	a, b := clientFor(apimTestPFX(t, "tenant-a")), clientFor(apimTestPFX(t, "tenant-b"))
	assert.NotSame(t, a.client, b.client, "different certificates must not share a client")
}

func TestAPIMRestore_AddsMissingSystemEntities(t *testing.T) {
	t.Parallel()

	now := time.Now()
	src := NewAPIMProvider()
	s := newAPIMService("sub", "rg", "old", now)
	s.Generic = map[string]map[string]*apimEntity{apimGroupsType: {
		entKey("", "guests"): {ID: "guests", Props: map[string]any{apimCDisplayName: "mine", fieldType: "custom"}},
	}}
	delete(s.Subs, entKey("", apimMasterSub))

	key := apimKey("sub", "rg", "old")
	src.svcs[key] = s

	dst := NewAPIMProvider()
	dst.restore(src.snapshot())

	got := dst.svcs[key]
	require.NotNil(t, got)

	for _, id := range []string{"administrators", "developers"} {
		assert.NotNil(t, got.Generic[apimGroupsType][entKey("", id)], id)
	}

	assert.Equal(t, "custom", strOf(got.Generic[apimGroupsType][entKey("", "guests")].Props, fieldType),
		"a user-created group is not overwritten")

	master := got.Subs[entKey("", apimMasterSub)]
	require.NotNil(t, master)

	dst2 := NewAPIMProvider()
	dst2.restore(dst.snapshot())
	assert.Equal(t, master.Props[redisPropPrimaryKey],
		dst2.svcs[key].Subs[entKey("", apimMasterSub)].Props[redisPropPrimaryKey], "restore is idempotent")
}
