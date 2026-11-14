package connect

import (
	"slices"
)

const (
	metricPosition = "POSITION_IN_QUEUE"
	metricWait     = "ESTIMATED_WAIT_TIME"
)

// ContactMetricInfo names one contact-level metric.
type ContactMetricInfo struct {
	Name string `json:"Name"`
}

// ContactMetricValue is the union holding a numeric metric value.
type ContactMetricValue struct {
	Number float64 `json:"Number"`
}

// ContactMetricResult is one computed contact metric.
type ContactMetricResult struct {
	Name  string             `json:"Name"`
	Value ContactMetricValue `json:"Value"`
}

// GetContactMetricsInput is the GetContactMetrics request.
type GetContactMetricsInput struct {
	ContactID  string              `json:"ContactId"`
	InstanceID string              `json:"InstanceId"`
	Metrics    []ContactMetricInfo `json:"Metrics"`
}

// ContactMetricsOut is the contact metrics response.
type ContactMetricsOut struct {
	Arn           string                `json:"Arn"`
	ID            string                `json:"Id"`
	MetricResults []ContactMetricResult `json:"MetricResults"`
}

// GetContactMetrics computes the queue position and expected wait of a waiting contact.
func (b *Backend) GetContactMetrics(in *GetContactMetricsInput) (*ContactMetricsOut, error) {
	if in.ContactID == "" || in.InstanceID == "" || len(in.Metrics) == 0 {
		return nil, invalidParamErr("ContactId, InstanceId and Metrics are required")
	}

	for _, m := range in.Metrics {
		if err := oneOf("Metrics.Name", m.Name, metricPosition, metricWait); err != nil {
			return nil, err
		}
	}

	return readInstance(b, "GetContactMetrics", in.InstanceID, func(inst string) (*ContactMetricsOut, error) {
		c, err := b.contacts.get(inst, in.ContactID)
		if err != nil {
			return nil, err
		}

		if !c.active() || c.queueID() == "" || c.agentID() != "" || enqueued(c) == nil {
			return nil, invalidRequestErr("contact %q is not waiting in a queue", in.ContactID)
		}

		position := b.queuePosition(inst, c)
		out := &ContactMetricsOut{Arn: c.Contact.Arn, ID: c.Contact.ID}

		for _, m := range in.Metrics {
			v := position
			if m.Name == metricWait {
				v = position * b.meanQueueAnswer(inst, c.queueID())
			}

			out.MetricResults = append(
				out.MetricResults,
				ContactMetricResult{Name: m.Name, Value: ContactMetricValue{Number: v}},
			)
		}

		return out, nil
	})
}

// queuePosition is 1 plus the waiting contacts of the queue that were enqueued earlier.
func (b *Backend) queuePosition(inst string, c *contactRec) float64 {
	ahead := countWhere(b.contacts.in(inst), func(o *contactRec) bool {
		return o != c && o.active() && o.queueID() == c.queueID() && o.agentID() == "" &&
			enqueued(o) != nil && (enqueued(o).Before(enqueued(c).Time) ||
			(enqueued(o).Equal(enqueued(c).Time) && o.Contact.ID < c.Contact.ID))
	})

	return ahead + 1
}

// meanQueueAnswer is the mean seconds contacts of the queue waited before reaching an agent.
func (b *Backend) meanQueueAnswer(inst, queueID string) float64 {
	cs := slices.DeleteFunc(b.contacts.in(inst), func(c *contactRec) bool { return c.queueID() != queueID })

	return meanOf(cs, func(c *contactRec) (float64, bool) { return span(enqueued(c), connected(c), true) })
}
