package iot_test

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMQTTAuthCertificateVariables(t *testing.T) {
	t.Parallel()

	subject := &pkix.Name{
		ExtraNames: []pkix.AttributeTypeAndValue{
			{Type: asn1.ObjectIdentifier{2, 5, 4, 6}, Value: "US"},
			{Type: asn1.ObjectIdentifier{2, 5, 4, 6}, Value: "CA"},
		},
		Organization: []string{"IoT Devices"}, OrganizationalUnit: []string{"LightBulb"},
		Province: []string{"NY"}, CommonName: defaultCN, SerialNumber: "123",
	}
	issuer := &pkix.Name{Country: []string{"DE"}, Organization: []string{"Primary CA"}, CommonName: "Primary Issuer"}
	wild := &pkix.Name{CommonName: "dev*ice", Organization: []string{"Acme"}}

	pubTo := func(topicARN string) string { return allowStmt("iot:Publish", "arn:aws:iot:*:*:topic/"+topicARN) }

	tests := []struct {
		name      string
		stmt      string
		topic     string
		spec      certSpec
		delivered bool
	}{
		{name: "subject_organization", spec: certSpec{subject: subject},
			stmt: pubTo("${iot:Certificate.Subject.Organization}/x"), topic: "IoT Devices/x", delivered: true},
		{name: "subject_state_and_ou", spec: certSpec{subject: subject},
			stmt:  pubTo("${iot:Certificate.Subject.State}/${iot:Certificate.Subject.OrganizationalUnit}"),
			topic: "NY/LightBulb", delivered: true},
		{name: "multi_value_first_is_default", spec: certSpec{subject: subject},
			stmt: pubTo("c/${iot:Certificate.Subject.Country}"), topic: "c/US", delivered: true},
		{name: "multi_value_index", spec: certSpec{subject: subject},
			stmt: pubTo("c/${iot:Certificate.Subject.Country.1}"), topic: "c/CA", delivered: true},
		{name: "multi_value_missing_index_denied", spec: certSpec{subject: subject},
			stmt: pubTo("c/${iot:Certificate.Subject.Country.2}"), topic: "c/CA"},
		{name: "multi_value_list_for_any_value", spec: certSpec{subject: subject}, topic: "c/x", delivered: true,
			stmt: condAllow(`{"ForAnyValue:StringEquals":{"iot:Certificate.Subject.Country.List":["CA","FR"]}}`)},
		{name: "multi_value_list_no_overlap", spec: certSpec{subject: subject}, topic: "c/x",
			stmt: condAllow(`{"ForAnyValue:StringEquals":{"iot:Certificate.Subject.Country.List":["FR"]}}`)},
		{name: "subject_serial_number_attribute", spec: certSpec{subject: subject},
			stmt: pubTo("s/${iot:Certificate.Subject.SerialNumber}"), topic: "s/123", delivered: true},
		{name: "issuer_common_name", spec: certSpec{subject: subject, issuer: issuer},
			stmt: pubTo("i/${iot:Certificate.Issuer.CommonName}"), topic: "i/Primary Issuer", delivered: true},
		{
			name:      "issuer_country_condition",
			spec:      certSpec{subject: subject, issuer: issuer},
			topic:     "c/x",
			delivered: true,
			stmt:      condAllow(`{"StringEquals":{"iot:Certificate.Issuer.Country":"DE"}}`),
		},
		{
			name:      "certificate_serial_number_decimal",
			spec:      certSpec{subject: subject, serial: big.NewInt(10525622389124227206 / 2)},
			stmt:      pubTo("n/${iot:Certificate.SerialNumber}"),
			topic:     "n/5262811194562113603",
			delivered: true,
		},
		{name: "issuer_alternative_dns", spec: certSpec{subject: subject, issuerAlt: "issuer.com"},
			stmt: pubTo("a/${iot:Certificate.Issuer.AlternativeName.DNSName}"), topic: "a/issuer.com", delivered: true},
		{
			name: "subject_alternative_dns",
			spec: certSpec{subject: subject, dns: []string{"example.com"}},
			stmt: pubTo(
				"d/${iot:Certificate.Subject.AlternativeName.DNSName}",
			),
			topic:     "d/example.com",
			delivered: true,
		},
		{name: "wildcard_value_not_substituted", spec: certSpec{subject: wild},
			stmt: pubTo("w/${iot:Certificate.Subject.CommonName}"), topic: "w/dev*ice"},
		{name: "missing_attribute_null_condition", spec: certSpec{subject: wild}, topic: "c/x", delivered: true,
			stmt: condAllow(`{"Null":{"iot:Certificate.Subject.Country":"true"}}`)},
		{name: "present_attribute_null_condition", spec: certSpec{subject: subject}, topic: "c/x", delivered: true,
			stmt: condAllow(`{"Null":{"iot:Certificate.Subject.Country":"false"}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newAuthEnv(t, true)
			spec := validSpec()
			spec.subject, spec.issuer, spec.serial, spec.dns = tt.spec.subject, tt.spec.issuer, tt.spec.serial, tt.spec.dns
			spec.issuerAlt = tt.spec.issuerAlt

			dev := env.customDevice(t, spec, "", "p", authPolicy(allowConnect, allowMarkerPub, tt.stmt))

			assert.Equal(t, tt.delivered, env.publishDelivered(t, dev, "dev1", tt.topic))
		})
	}
}
