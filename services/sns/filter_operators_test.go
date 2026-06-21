package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// attr is a shorthand for a [DataType, StringValue] message-attribute pair.
func attr(dataType, value string) [2]string { return [2]string{dataType, value} }

// TestFilterPolicy_OrOperator covers the SNS "$or" operator across message
// attributes, mirroring the AWS developer-guide examples:
//
//	"source" && ("metricName" || "namespace")
func TestFilterPolicy_OrOperator(t *testing.T) {
	t.Parallel()

	policy := `{
		"source": ["aws.cloudwatch"],
		"$or": [
			{"metricName": ["CPUUtilization"]},
			{"namespace": ["AWS/EC2"]}
		]
	}`

	tests := []struct {
		attrs map[string][2]string
		name  string
		want  bool
	}{
		{
			name: "source and first or-branch",
			attrs: map[string][2]string{
				"source":     attr("String", "aws.cloudwatch"),
				"metricName": attr("String", "CPUUtilization"),
			},
			want: true,
		},
		{
			name: "source and second or-branch",
			attrs: map[string][2]string{
				"source":    attr("String", "aws.cloudwatch"),
				"namespace": attr("String", "AWS/EC2"),
			},
			want: true,
		},
		{
			name: "source present but neither or-branch matches",
			attrs: map[string][2]string{
				"source":     attr("String", "aws.cloudwatch"),
				"metricName": attr("String", "ReadLatency"),
			},
			want: false,
		},
		{
			name: "or-branch matches but mandatory source missing",
			attrs: map[string][2]string{
				"metricName": attr("String", "CPUUtilization"),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyAttributesForTest(policy, tt.attrs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_OrNotRecognised verifies that "$or" is treated as an ordinary
// attribute name when AWS recognition rules are not met (fewer than 2 objects, or
// an object using a reserved keyword as a field name).
func TestFilterPolicy_OrNotRecognised(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs  map[string][2]string
		name   string
		policy string
		want   bool
	}{
		{
			name:   "single-element $or is a literal attribute name",
			policy: `{"$or": ["literal-value"]}`,
			attrs:  map[string][2]string{"$or": attr("String", "literal-value")},
			want:   true,
		},
		{
			name:   "reserved keyword field disables $or semantics",
			policy: `{"$or": [{"numeric": [">", 1]}, {"prefix": "abc"}]}`,
			// Treated as literal attribute "$or"; absent here so it cannot match.
			attrs: map[string][2]string{"other": attr("String", "x")},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyAttributesForTest(tt.policy, tt.attrs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_CIDR covers IP-address matching via the "cidr" operator.
func TestFilterPolicy_CIDR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		value  string
		want   bool
	}{
		{"in range low", `{"source_ip":[{"cidr":"10.0.0.0/24"}]}`, "10.0.0.0", true},
		{"in range high", `{"source_ip":[{"cidr":"10.0.0.0/24"}]}`, "10.0.0.255", true},
		{"out of range", `{"source_ip":[{"cidr":"10.0.0.0/24"}]}`, "10.1.1.0", false},
		{"bare host ip match", `{"source_ip":[{"cidr":"192.168.1.1"}]}`, "192.168.1.1", true},
		{"bare host ip mismatch", `{"source_ip":[{"cidr":"192.168.1.1"}]}`, "192.168.1.2", false},
		{"non-ip value", `{"source_ip":[{"cidr":"10.0.0.0/24"}]}`, "not-an-ip", false},
		{"ipv6 in range", `{"source_ip":[{"cidr":"2001:db8::/32"}]}`, "2001:db8::1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyAttributesForTest(
				tt.policy, map[string][2]string{"source_ip": attr("String", tt.value)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_Wildcard covers the "wildcard" operator.
func TestFilterPolicy_Wildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{"trailing star", "order-*", "order-123", true},
		{"leading star", "*ball", "baseball", true},
		{"leading star no match", "*ball", "basket", false},
		{"middle star", "a*z", "abcz", true},
		{"middle star no match", "a*z", "abcy", false},
		{"two stars", "*-*", "left-right", true},
		{"no star exact", "exact", "exact", true},
		{"no star mismatch", "exact", "exacto", false},
		{"star matches empty", "abc*", "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policy := `{"k":[{"wildcard":"` + tt.pattern + `"}]}`
			got, err := sns.MatchesFilterPolicyAttributesForTest(
				policy, map[string][2]string{"k": attr("String", tt.value)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_AnythingButNested covers anything-but combined with prefix,
// suffix, equals-ignore-case, and wildcard.
func TestFilterPolicy_AnythingButNested(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		key    string
		value  string
		want   bool
	}{
		{
			"anything-but prefix denies",
			`{"event":[{"anything-but":{"prefix":"order-"}}]}`,
			"event",
			"order-cancelled",
			false,
		},
		{"anything-but prefix allows", `{"event":[{"anything-but":{"prefix":"order-"}}]}`, "event", "data-entry", true},
		{"anything-but suffix denies", `{"i":[{"anything-but":{"suffix":"ball"}}]}`, "i", "baseball", false},
		{"anything-but suffix allows", `{"i":[{"anything-but":{"suffix":"ball"}}]}`, "i", "hockey", true},
		{"anything-but eq-ic denies", `{"i":[{"anything-but":{"equals-ignore-case":"tennis"}}]}`, "i", "TENNIS", false},
		{"anything-but eq-ic allows", `{"i":[{"anything-but":{"equals-ignore-case":"tennis"}}]}`, "i", "rugby", true},
		{"anything-but wildcard denies", `{"i":[{"anything-but":{"wildcard":"*ball"}}]}`, "i", "basketball", false},
		{"anything-but wildcard allows", `{"i":[{"anything-but":{"wildcard":"*ball"}}]}`, "i", "hockey", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyAttributesForTest(
				tt.policy, map[string][2]string{tt.key: attr("String", tt.value)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_StringArray verifies that a String.Array attribute matches if
// ANY array element satisfies the condition.
func TestFilterPolicy_StringArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		value  string
		want   bool
	}{
		{"exact element present", `{"i":["rugby","tennis"]}`, `["rugby","baseball"]`, true},
		{"exact element absent", `{"i":["rugby","tennis"]}`, `["baseball","football"]`, false},
		{"prefix element matches", `{"i":[{"prefix":"bas"}]}`, `["rugby","baseball"]`, true},
		{"anything-but excludes when element present", `{"i":[{"anything-but":["rugby"]}]}`, `["rugby"]`, false},
		{
			"anything-but allows when other element present",
			`{"i":[{"anything-but":["rugby"]}]}`,
			`["rugby","baseball"]`,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyAttributesForTest(
				tt.policy, map[string][2]string{"i": attr("String.Array", tt.value)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_OrMessageBody exercises "$or" plus array/boolean handling in
// MessageBody scope.
func TestFilterPolicy_OrMessageBody(t *testing.T) {
	t.Parallel()

	policy := `{
		"source": ["aws.cloudwatch"],
		"$or": [
			{"metricName": ["CPUUtilization"]},
			{"namespace": ["AWS/EC2"]}
		]
	}`

	tests := []struct {
		name string
		body string
		want bool
	}{
		{"first branch", `{"source":"aws.cloudwatch","metricName":"CPUUtilization"}`, true},
		{"second branch", `{"source":"aws.cloudwatch","namespace":"AWS/EC2"}`, true},
		{"no branch", `{"source":"aws.cloudwatch","metricName":"Other"}`, false},
		{"missing source", `{"metricName":"CPUUtilization"}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyMessageBodyForTest(policy, tt.body)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFilterPolicy_MessageBodyArrayAndBool verifies array and boolean value
// handling in MessageBody scope.
func TestFilterPolicy_MessageBodyArrayAndBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		body   string
		want   bool
	}{
		{"array element matches", `{"interests":["rugby"]}`, `{"interests":["rugby","baseball"]}`, true},
		{"array element absent", `{"interests":["rugby"]}`, `{"interests":["baseball"]}`, false},
		{"boolean true matches", `{"enabled":["true"]}`, `{"enabled":true}`, true},
		{"boolean false mismatch", `{"enabled":["true"]}`, `{"enabled":false}`, false},
		{"cidr in body", `{"ip":[{"cidr":"10.0.0.0/8"}]}`, `{"ip":"10.5.6.7"}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyMessageBodyForTest(tt.policy, tt.body)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
