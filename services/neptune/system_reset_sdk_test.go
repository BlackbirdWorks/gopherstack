package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNeptunedataFastReset(t *testing.T) {
	t.Parallel()

	_, c := newStreamsBackend(t)
	cl := dataClient(clusterAddr(c))

	sparqlUpdate(t, c, `INSERT DATA { <urn:s> <urn:p> <urn:o> }`)
	require.NoError(t, <-gremlinG(t, c).AddV("Gone").Iterate())

	first, err := cl.ExecuteFastReset(
		t.Context(),
		&neptunedata.ExecuteFastResetInput{Action: types.ActionInitializeReset},
	)
	require.NoError(t, err)
	assert.Equal(t, "200 OK", aws.ToString(first.Status))
	require.NotEmpty(t, aws.ToString(first.Payload.Token))

	second, err := cl.ExecuteFastReset(
		t.Context(),
		&neptunedata.ExecuteFastResetInput{Action: types.ActionInitializeReset},
	)
	require.NoError(t, err)

	tests := []struct {
		name  string
		in    neptunedata.ExecuteFastResetInput
		code  string
		after int
	}{
		{
			"superseded token",
			neptunedata.ExecuteFastResetInput{Action: types.ActionPerformReset, Token: first.Payload.Token},
			"InvalidParameterException",
			1,
		},
		{
			"no token",
			neptunedata.ExecuteFastResetInput{Action: types.ActionPerformReset},
			"MissingParameterException",
			1,
		},
		{"bad action", neptunedata.ExecuteFastResetInput{Action: "wipe"}, "InvalidParameterException", 1},
	}

	for _, tt := range tests {
		_, perr := cl.ExecuteFastReset(t.Context(), &tt.in)
		require.Error(t, perr, tt.name)
		assert.Equal(t, tt.code, errCode(t, perr), tt.name)
	}

	out, err := cl.GetSparqlStream(
		t.Context(),
		&neptunedata.GetSparqlStreamInput{IteratorType: types.IteratorTypeTrimHorizon},
	)
	require.NoError(t, err)
	require.Len(t, out.Records, 1, "rejected resets leave data alone")

	done, err := cl.ExecuteFastReset(t.Context(), &neptunedata.ExecuteFastResetInput{
		Action: types.ActionPerformReset, Token: second.Payload.Token,
	})
	require.NoError(t, err)
	assert.Equal(t, "200 OK", aws.ToString(done.Status))

	_, err = cl.ExecuteFastReset(t.Context(), &neptunedata.ExecuteFastResetInput{
		Action: types.ActionPerformReset, Token: second.Payload.Token,
	})
	require.Error(t, err, "a token is single use")

	res := sparqlDo(
		t,
		c,
		"GET",
		"/sparql?query=SELECT+*+WHERE+%7B%3Fs+%3Fp+%3Fo%7D",
		"",
		"application/sparql-results+json",
		"",
	)
	assert.NotContains(t, res.body, "urn:s")

	cnt := sparqlDo(
		t,
		c,
		"POST",
		"/openCypher",
		"application/x-www-form-urlencoded",
		"",
		"query=MATCH+(n)+RETURN+count(n)+AS+c",
	)
	assert.Contains(t, cnt.body, `"c":0`)

	_, err = cl.GetSparqlStream(t.Context(), &neptunedata.GetSparqlStreamInput{CommitNum: aws.Int64(1)})
	require.Error(t, err)
	assert.Equal(t, "StreamRecordsNotFoundException", errCode(t, err), "streams restart from scratch")

	form := sparqlDo(t, c, "POST", "/system", "application/x-www-form-urlencoded", "", "action=initiateDatabaseReset")
	assert.Equal(t, 200, form.status)
	assert.Contains(t, form.body, `"token"`)

	dup := sparqlDo(t, c, "POST", "/system", "application/x-www-form-urlencoded", "", "action=a&action=b")
	assert.Equal(t, 400, dup.status)
	assert.Contains(t, dup.body, "Too many values supplied for: action")

	get := sparqlDo(t, c, "GET", "/system", "", "", "")
	assert.Equal(t, 405, get.status)
}
