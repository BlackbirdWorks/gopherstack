package sts_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

func TestEvaluateAssumeRoleTrust_ExtendedOperators(t *testing.T) {
	t.Parallel()

	const caller = "arn:aws:iam::123456789012:user/alice"

	policy := func(op, key, value string) string {
		return `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},` +
			`"Action":"sts:AssumeRole","Condition":{"` + op + `":{"` + key + `":` + value + `}}}]}`
	}

	tests := []struct {
		name    string
		op      string
		key     string
		want    string
		ctxKey  string
		ctxVal  string
		wantErr bool
	}{
		{
			name:    "num_eq",
			op:      "NumericEquals",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "5",
			wantErr: false,
		},
		{
			name:    "num_eq_miss",
			op:      "NumericEquals",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "6",
			wantErr: true,
		},
		{
			name:    "num_ne",
			op:      "NumericNotEquals",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "6",
			wantErr: false,
		},
		{
			name:    "num_lt",
			op:      "NumericLessThan",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "4",
			wantErr: false,
		},
		{
			name:    "num_lt_eq_miss",
			op:      "NumericLessThan",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "5",
			wantErr: true,
		},
		{
			name:    "num_lte",
			op:      "NumericLessThanEquals",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "5",
			wantErr: false,
		},
		{
			name:    "num_gt",
			op:      "NumericGreaterThan",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "6",
			wantErr: false,
		},
		{
			name:    "num_gt_miss",
			op:      "NumericGreaterThan",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "5",
			wantErr: true,
		},
		{
			name:    "num_gte",
			op:      "NumericGreaterThanEquals",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "5",
			wantErr: false,
		},
		{
			name:    "num_any_of",
			op:      "NumericEquals",
			key:     "aws:EpochTime",
			want:    `["1","5"]`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "5",
			wantErr: false,
		},
		{
			name:    "num_bad_actual",
			op:      "NumericEquals",
			key:     "aws:EpochTime",
			want:    `"5"`,
			ctxKey:  "aws:epochtime",
			ctxVal:  "x",
			wantErr: true,
		},
		{
			name:    "ip_cidr",
			op:      "IpAddress",
			key:     "aws:SourceIp",
			want:    `"10.0.0.0/8"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "10.1.2.3",
			wantErr: false,
		},
		{
			name:    "ip_cidr_miss",
			op:      "IpAddress",
			key:     "aws:SourceIp",
			want:    `"10.0.0.0/8"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "11.1.2.3",
			wantErr: true,
		},
		{
			name:    "ip_bare",
			op:      "IpAddress",
			key:     "aws:SourceIp",
			want:    `"203.0.113.7"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "203.0.113.7",
			wantErr: false,
		},
		{
			name:    "ip_v6",
			op:      "IpAddress",
			key:     "aws:SourceIp",
			want:    `"2001:db8::/32"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "2001:db8::1",
			wantErr: false,
		},
		{
			name:    "not_ip_denies",
			op:      "NotIpAddress",
			key:     "aws:SourceIp",
			want:    `"10.0.0.0/8"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "10.1.2.3",
			wantErr: true,
		},
		{
			name:    "not_ip_allows",
			op:      "NotIpAddress",
			key:     "aws:SourceIp",
			want:    `"10.0.0.0/8"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "192.168.1.1",
			wantErr: false,
		},
		{
			name:    "binary_eq",
			op:      "BinaryEquals",
			key:     "sts:ExternalId",
			want:    `"aGVsbG8="`,
			ctxKey:  "sts:externalid",
			ctxVal:  "aGVsbG8=",
			wantErr: false,
		},
		{
			name:    "binary_miss",
			op:      "BinaryEquals",
			key:     "sts:ExternalId",
			want:    `"aGVsbG8="`,
			ctxKey:  "sts:externalid",
			ctxVal:  "d29ybGQ=",
			wantErr: true,
		},
		{
			name:    "ifexists_absent",
			op:      "IpAddressIfExists",
			key:     "aws:SourceIp",
			want:    `"10.0.0.0/8"`,
			ctxKey:  "",
			ctxVal:  "",
			wantErr: false,
		},
		{
			name:    "ifexists_present_miss",
			op:      "IpAddressIfExists",
			key:     "aws:SourceIp",
			want:    `"10.0.0.0/8"`,
			ctxKey:  "aws:sourceip",
			ctxVal:  "11.0.0.1",
			wantErr: true,
		},
		{
			name:    "for_all_values",
			op:      "ForAllValues:StringEquals",
			key:     "aws:PrincipalArn",
			want:    `["a","b"]`,
			ctxKey:  "aws:principalarn",
			ctxVal:  "a,b",
			wantErr: false,
		},
		{
			name:    "for_all_values_miss",
			op:      "ForAllValues:StringEquals",
			key:     "aws:PrincipalArn",
			want:    `["a"]`,
			ctxKey:  "aws:principalarn",
			ctxVal:  "a,b",
			wantErr: true,
		},
		{
			name:    "for_any_value",
			op:      "ForAnyValue:StringEquals",
			key:     "aws:PrincipalArn",
			want:    `["b"]`,
			ctxKey:  "aws:principalarn",
			ctxVal:  "a,b",
			wantErr: false,
		},
		{
			name:    "for_any_value_miss",
			op:      "ForAnyValue:StringEquals",
			key:     "aws:PrincipalArn",
			want:    `["c"]`,
			ctxKey:  "aws:principalarn",
			ctxVal:  "a,b",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var ctx map[string]string
			if tt.ctxKey != "" {
				ctx = map[string]string{tt.ctxKey: tt.ctxVal}
			}

			err := sts.EvaluateAssumeRoleTrust(policy(tt.op, tt.key, tt.want), sts.TrustEvalForTest{
				Action: sts.ActionAssumeRole, CallerArn: caller, ConditionCtx: ctx,
			})
			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAssumeRole_SourceIPCondition(t *testing.T) {
	t.Parallel()

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"sts:AssumeRole","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`

	tests := []struct {
		name    string
		ip      string
		wantErr bool
	}{
		{
			name: "inside",
			ip:   "10.2.3.4",
		},
		{
			name:    "outside",
			ip:      "198.51.100.9",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
				RoleSessionName: "session",
				CallerArn:       "arn:aws:iam::123456789012:user/alice",
				SourceIP:        tt.ip,
			})
			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)

				return
			}

			assert.NoError(t, err)
		})
	}
}
