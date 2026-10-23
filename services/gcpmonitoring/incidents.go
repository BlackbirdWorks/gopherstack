package gcpmonitoring

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	stateOpen    = "open"
	stateClosed  = "closed"
	maxIncidents = 1000

	incidentSuffixLen = 12
)

// Incident is one alert firing: it opens when a condition is met and closes when it recovers.
type Incident struct {
	StartedAt            time.Time         `json:"startedAt"`
	EndedAt              time.Time         `json:"endedAt"`
	MetricLabels         map[string]string `json:"metricLabels,omitempty"`
	ResourceLabels       map[string]string `json:"resourceLabels,omitempty"`
	ID                   string            `json:"id"`
	Key                  string            `json:"key"`
	Project              string            `json:"project"`
	Policy               string            `json:"policy"`
	PolicyDisplayName    string            `json:"policyDisplayName"`
	Condition            string            `json:"condition"`
	ConditionDisplayName string            `json:"conditionDisplayName"`
	State                string            `json:"state"`
	MetricType           string            `json:"metricType"`
	ResourceType         string            `json:"resourceType"`
	Observed             float64           `json:"observed"`
	Threshold            float64           `json:"threshold"`
}

func newIncidentID() string { return "0." + awsid.NewN("", incidentSuffixLen) }

func cloneIncident(i *Incident) Incident {
	out := *i

	return out
}

// Incidents returns the project's incidents, oldest first; open ones have a zero EndedAt.
func (b *InMemoryBackend) Incidents(project string) []Incident {
	b.mu.RLock("Incidents")
	defer b.mu.RUnlock()

	var out []Incident

	for _, i := range b.incidents {
		if i.Project == project {
			out = append(out, cloneIncident(i))
		}
	}

	sort.Slice(out, func(x, y int) bool {
		if !out[x].StartedAt.Equal(out[y].StartedAt) {
			return out[x].StartedAt.Before(out[y].StartedAt)
		}

		return out[x].ID < out[y].ID
	})

	return out
}

func (b *InMemoryBackend) openIncidentLocked(key string) *Incident {
	for _, i := range b.incidents {
		if i.Key == key && i.State == stateOpen {
			return i
		}
	}

	return nil
}

// pruneIncidentsLocked drops the oldest closed incidents beyond the cap.
func (b *InMemoryBackend) pruneIncidentsLocked() {
	if len(b.incidents) <= maxIncidents {
		return
	}

	closed := make([]*Incident, 0, len(b.incidents))

	for _, i := range b.incidents {
		if i.State == stateClosed {
			closed = append(closed, i)
		}
	}

	sort.Slice(closed, func(x, y int) bool { return closed[x].EndedAt.Before(closed[y].EndedAt) })

	for _, i := range closed {
		if len(b.incidents) <= maxIncidents {
			return
		}

		delete(b.incidents, i.ID)
	}
}
