package iot_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"math/big"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

const defaultCN = "device-7"

type certSpec struct {
	notBefore time.Time
	notAfter  time.Time
	subject   *pkix.Name
	issuer    *pkix.Name
	serial    *big.Int
	cn        string
	issuerAlt string
	dns       []string
}

func validSpec() certSpec {
	now := time.Now()

	return certSpec{cn: defaultCN, notBefore: now.Add(-time.Hour), notAfter: now.Add(time.Hour)}
}

func (e *authEnv) customDevice(t *testing.T, spec certSpec, thing, policyName, policyDoc string) authDevice {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: spec.cn},
		NotBefore:    spec.notBefore,
		NotAfter:     spec.notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     spec.dns,
	}

	if spec.subject != nil {
		tmpl.Subject = *spec.subject
	}

	if spec.issuerAlt != "" {
		name, mErr := asn1.Marshal(
			asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 2, Bytes: []byte(spec.issuerAlt)},
		)
		require.NoError(t, mErr)

		seq, mErr := asn1.Marshal(
			asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true, Bytes: name},
		)
		require.NoError(t, mErr)

		tmpl.ExtraExtensions = append(
			tmpl.ExtraExtensions,
			pkix.Extension{Id: asn1.ObjectIdentifier{2, 5, 29, 18}, Value: seq},
		)
	}

	if spec.serial != nil {
		tmpl.SerialNumber = spec.serial
	}

	parent, signer := tmpl, key

	if spec.issuer != nil {
		issuerKey, ikErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, ikErr)

		parent = &x509.Certificate{
			SerialNumber: big.NewInt(1), Subject: *spec.issuer, NotBefore: spec.notBefore, NotAfter: spec.notAfter,
		}
		signer = issuerKey
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, key.Public(), signer)
	require.NoError(t, err)

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))

	cert, err := e.backend.RegisterCertificateWithoutCA(&iot.RegisterCertificateInput{
		CertificatePem: certPEM, Status: "ACTIVE",
	})
	require.NoError(t, err)

	_, err = e.backend.CreatePolicy(&iot.CreatePolicyInput{PolicyName: policyName, PolicyDocument: policyDoc})
	require.NoError(t, err)
	require.NoError(t, e.backend.AttachPolicy(&iot.AttachPolicyInput{PolicyName: policyName, Target: cert.ARN}))

	if thing != "" {
		require.NoError(t, e.backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
			ThingName: thing, Principal: cert.ARN,
		}))
	}

	return authDevice{cert: *cert, key: keyPEM}
}

func (e *authEnv) createThing(t *testing.T, name, thingType string, attrs map[string]string) {
	t.Helper()

	if thingType != "" {
		_, err := e.backend.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: thingType})
		require.NoError(t, err)
	}

	_, err := e.backend.CreateThing(&iot.CreateThingInput{
		ThingName:        name,
		ThingTypeName:    thingType,
		AttributePayload: &iot.AttributePayload{Attributes: attrs},
	})
	require.NoError(t, err)
}

func (e *authEnv) publishDelivered(t *testing.T, pub authDevice, clientID, topic string) bool {
	t.Helper()

	obs := e.device(t, true, "", "obs-policy", authPolicy(allowAll))
	msgs := make(chan pahomqtt.Message, 8)

	observer, err := e.connect(t, obs, "observer", msgs)
	require.NoError(t, err)
	require.NoError(t, subscribeErr(observer, topic))
	require.NoError(t, subscribeErr(observer, authMarker))

	c, err := e.connect(t, pub, clientID, nil)
	require.NoError(t, err)

	require.True(t, c.Publish(topic, 0, false, "x").WaitTimeout(authDeadline))
	require.True(t, c.Publish(authMarker, 0, false, "m").WaitTimeout(authDeadline))

	var got []string

	timer := time.NewTimer(authDeadline)
	defer timer.Stop()

	for len(got) == 0 || got[len(got)-1] != authMarker {
		select {
		case m := <-msgs:
			got = append(got, m.Topic())
		case <-timer.C:
			require.FailNow(t, "marker never delivered", "got %v", got)
		}
	}

	return len(got) == 2
}

func condAllow(cond string) string {
	return fmt.Sprintf(
		`{"Effect":"Allow","Action":"iot:Publish","Resource":"*","Condition":%s}`, cond,
	)
}

func TestMQTTAuthThingGroupPolicy(t *testing.T) {
	t.Parallel()

	const topic = "g/topic"

	groupAllow := authPolicy(allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/g/*"))
	groupDeny := authPolicy(denyStmt("iot:Publish", "arn:aws:iot:*:*:topic/g/*"))

	tests := []struct {
		name        string
		groupPolicy string
		certStmt    string
		attachTo    string
		attached    bool
		delivered   bool
	}{
		{name: "member_group", groupPolicy: groupAllow, attachTo: "child", attached: true, delivered: true},
		{name: "parent_group", groupPolicy: groupAllow, attachTo: "parent", attached: true, delivered: true},
		{name: "unrelated_group", groupPolicy: groupAllow, attachTo: "other", attached: true},
		{name: "cert_not_attached_to_thing", groupPolicy: groupAllow, attachTo: "child"},
		{
			name: "group_deny_wins", groupPolicy: groupDeny, attachTo: "parent", attached: true,
			certStmt: allowStmt("iot:Publish", "*"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newAuthEnv(t, true)
			certStmt := tt.certStmt
			if certStmt == "" {
				certStmt = allowMarkerPub
			}

			thing := ""
			if tt.attached {
				thing = "dev1"
			}

			env.createThing(t, "dev1", "", nil)

			dev := env.customDevice(t, validSpec(), thing, "cert-policy",
				authPolicy(allowConnect, allowMarkerPub, certStmt))

			groups := map[string]*iot.ThingGroup{}

			for _, g := range []struct{ name, parent string }{
				{"parent", ""}, {"child", "parent"}, {"other", ""},
			} {
				tg, err := env.backend.CreateThingGroup(&iot.CreateThingGroupInput{
					ThingGroupName: g.name, ParentGroupName: g.parent,
				})
				require.NoError(t, err)

				groups[g.name] = tg
			}

			require.NoError(t, env.backend.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
				ThingName: "dev1", ThingGroupName: "child",
			}))

			_, err := env.backend.CreatePolicy(&iot.CreatePolicyInput{
				PolicyName: "group-policy", PolicyDocument: tt.groupPolicy,
			})
			require.NoError(t, err)
			require.NoError(t, env.backend.AttachPolicy(&iot.AttachPolicyInput{
				PolicyName: "group-policy", Target: groups[tt.attachTo].ThingGroupARN,
			}))

			assert.Equal(t, tt.delivered, env.publishDelivered(t, dev, "dev1", topic))
		})
	}
}

func TestMQTTAuthCertificateValidity(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name string
		spec certSpec
		ok   bool
	}{
		{name: "valid", spec: validSpec(), ok: true},
		{
			name: "expired",
			spec: certSpec{cn: defaultCN, notBefore: now.Add(-2 * time.Hour), notAfter: now.Add(-time.Hour)},
		},
		{
			name: "not_yet_valid",
			spec: certSpec{cn: defaultCN, notBefore: now.Add(time.Hour), notAfter: now.Add(2 * time.Hour)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newAuthEnv(t, true)
			dev := env.customDevice(t, tt.spec, "", "p", authPolicy(allowAll))

			_, err := env.connect(t, dev, "dev1", nil)
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestMQTTAuthPolicyContext(t *testing.T) {
	t.Parallel()

	attached := `{"Bool":{"iot:Connection.Thing.IsAttached":"true"}}`

	tests := []struct {
		attrs     map[string]string
		name      string
		stmt      string
		thingType string
		topic     string
		withThing bool
		delivered bool
	}{
		{
			name: "attribute_variable_match", withThing: true, attrs: map[string]string{"site": "lab"},
			stmt:  allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/a/${iot:Connection.Thing.Attributes[site]}/x"),
			topic: "a/lab/x", delivered: true,
		},
		{
			name: "attribute_variable_mismatch", withThing: true, attrs: map[string]string{"site": "lab"},
			stmt:  allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/a/${iot:Connection.Thing.Attributes[site]}/x"),
			topic: "a/other/x",
		},
		{
			name: "thing_type_variable", withThing: true, thingType: "sensor",
			stmt:  allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/t/${iot:Connection.Thing.ThingTypeName}"),
			topic: "t/sensor", delivered: true,
		},
		{
			name: "thing_type_variable_mismatch", withThing: true, thingType: "sensor",
			stmt:  allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/t/${iot:Connection.Thing.ThingTypeName}"),
			topic: "t/actuator",
		},
		{
			name:  "common_name_variable",
			stmt:  allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/cn/${iot:Certificate.Subject.CommonName}"),
			topic: "cn/" + defaultCN, delivered: true,
		},
		{
			name:  "common_name_variable_mismatch",
			stmt:  allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/cn/${iot:Certificate.Subject.CommonName}"),
			topic: "cn/someone-else",
		},
		{
			name: "is_attached_true", withThing: true, topic: "c/x", delivered: true,
			stmt: condAllow(attached),
		},
		{name: "is_attached_false", topic: "c/x", stmt: condAllow(attached)},
		{
			name: "string_equals_common_name", topic: "c/x", delivered: true,
			stmt: condAllow(`{"StringEquals":{"iot:Certificate.Subject.CommonName":"` + defaultCN + `"}}`),
		},
		{
			name:  "string_equals_common_name_mismatch",
			topic: "c/x",
			stmt:  condAllow(`{"StringEquals":{"iot:Certificate.Subject.CommonName":"nope"}}`),
		},
		{
			name: "string_like_attribute", withThing: true, attrs: map[string]string{"site": "lab-3"}, topic: "c/x",
			delivered: true,
			stmt:      condAllow(`{"StringLike":{"iot:Connection.Thing.Attributes[site]":"lab-*"}}`),
		},
		{
			name: "string_like_attribute_mismatch", withThing: true, attrs: map[string]string{"site": "prod-3"},
			topic: "c/x",
			stmt:  condAllow(`{"StringLike":{"iot:Connection.Thing.Attributes[site]":"lab-*"}}`),
		},
		{
			name: "string_equals_thing_type", withThing: true, thingType: "sensor", topic: "c/x", delivered: true,
			stmt: condAllow(`{"StringEquals":{"iot:Connection.Thing.ThingTypeName":"sensor"}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newAuthEnv(t, true)
			thing := ""

			if tt.withThing {
				thing = "dev1"
				env.createThing(t, thing, tt.thingType, tt.attrs)
			}

			dev := env.customDevice(
				t,
				validSpec(),
				thing,
				"p",
				authPolicy(allowConnect, allowMarkerPub, tt.stmt),
			)

			assert.Equal(t, tt.delivered, env.publishDelivered(t, dev, "dev1", tt.topic))
		})
	}
}
