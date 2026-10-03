package waf_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// TestWAFClassicResourceConcurrentWithUpdate proves Get/Create must not hand
// back the live pointer whose entry slice the shared Update path mutates.
func TestWAFClassicResourceConcurrentWithUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *waf.InMemoryBackend, token string) string
		reader  func(b *waf.InMemoryBackend, id string)
		mutator func(b *waf.InMemoryBackend, id, token string, i int)
		name    string
	}{
		{
			name: "WebACL races UpdateWebACL",
			setup: func(t *testing.T, b *waf.InMemoryBackend, token string) string {
				t.Helper()

				acl, err := b.CreateWebACL("race-acl", "raceMetric", waf.WafAction{Type: "ALLOW"}, token, nil)
				require.NoError(t, err)

				return acl.WebACLId
			},
			reader: func(b *waf.InMemoryBackend, id string) {
				got, err := b.GetWebACL(id)
				if err != nil {
					return
				}

				for _, r := range got.Rules {
					_ = r.RuleId
				}
			},
			mutator: func(b *waf.InMemoryBackend, id, token string, i int) {
				update := waf.WebACLUpdate{
					Action:        "INSERT",
					ActivatedRule: waf.ActivatedRule{RuleId: "rule-race", Priority: int32(i)},
				}
				_ = b.UpdateWebACL(id, token, nil, []waf.WebACLUpdate{update})

				update.Action = "DELETE"
				_ = b.UpdateWebACL(id, token, nil, []waf.WebACLUpdate{update})
			},
		},
		{
			name: "IPSet races UpdateIPSet (shared applyEntryUpdate helper)",
			setup: func(t *testing.T, b *waf.InMemoryBackend, token string) string {
				t.Helper()

				ipSet, err := b.CreateIPSet("race-ipset", token, nil)
				require.NoError(t, err)

				return ipSet.IPSetId
			},
			reader: func(b *waf.InMemoryBackend, id string) {
				got, err := b.GetIPSet(id)
				if err != nil {
					return
				}

				for _, d := range got.IPSetDescriptors {
					_ = d.Value
				}
			},
			mutator: func(b *waf.InMemoryBackend, id, token string, _ int) {
				descriptor := waf.IPSetDescriptor{Type: "IPV4", Value: "10.0.0.0/8"}

				_ = b.UpdateIPSet(id, token, []waf.IPSetUpdate{{Action: "INSERT", IPSetDescriptor: descriptor}})
				_ = b.UpdateIPSet(id, token, []waf.IPSetUpdate{{Action: "DELETE", IPSetDescriptor: descriptor}})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := waf.NewInMemoryBackend("000000000000", "us-east-1")
			token := b.GetChangeToken()
			id := tt.setup(t, b, token)

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(b, id)
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					tt.mutator(b, id, token, i)
				}
			}()

			wg.Wait()
		})
	}
}
