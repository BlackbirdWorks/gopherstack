package cloudwatch_test

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeAlarm_StateTransitionedTimestamp_InXMLResponse(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutCompositeAlarm&AlarmName=comp&AlarmRule=FALSE")
	postForm(t, h, "Action=SetAlarmState&AlarmName=comp&StateValue=ALARM&StateReason=manual")

	rec := postForm(t, h, "Action=DescribeAlarms&AlarmNames.member.1=comp")
	require.Equal(t, 200, rec.Code)

	type alarm struct {
		AlarmName                  string `xml:"AlarmName"`
		StateTransitionedTimestamp string `xml:"StateTransitionedTimestamp"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeAlarmsResponse"`
		Alarms  []alarm  `xml:"DescribeAlarmsResult>CompositeAlarms>member"`
	}
	var out resp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Alarms, 1)
	assert.NotEmpty(t, out.Alarms[0].StateTransitionedTimestamp,
		"StateTransitionedTimestamp must appear in DescribeAlarms XML for composite alarms")
}
