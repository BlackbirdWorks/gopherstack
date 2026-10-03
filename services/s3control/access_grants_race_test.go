package s3control_test

import (
	"sync"
	"testing"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestAccessGrantsInstanceConcurrentWithAssociate proves the instance
// getters must not hand back the live pointer Associate...IdentityCenter mutates.
func TestAccessGrantsInstanceConcurrentWithAssociate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(b *s3control.InMemoryBackend, accountID string)
		name   string
	}{
		{
			name: "GetAccessGrantsInstance races associate",
			reader: func(b *s3control.InMemoryBackend, accountID string) {
				inst, err := b.GetAccessGrantsInstance(accountID)
				if err != nil {
					return
				}

				_ = inst.IdentityCenterArn
			},
		},
		{
			name: "ListAccessGrantsInstances races associate",
			reader: func(b *s3control.InMemoryBackend, accountID string) {
				for _, inst := range b.ListAccessGrantsInstances(accountID) {
					_ = inst.IdentityCenterArn
				}
			},
		},
		{
			name: "GetAccessGrantsInstanceForPrefix races associate",
			reader: func(b *s3control.InMemoryBackend, accountID string) {
				inst, err := b.GetAccessGrantsInstanceForPrefix(accountID, "prefix")
				if err != nil {
					return
				}

				_ = inst.IdentityCenterArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			const accountID = "000000000000"
			b.CreateAccessGrantsInstance(accountID, "")

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(b, accountID)
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					b.AssociateAccessGrantsIdentityCenter(accountID, "arn:aws:sso:::instance/ssoins-1")
				}
			}()

			wg.Wait()
		})
	}
}

// TestAccessGrantsLocationConcurrentWithUpdate proves GetAccessGrantsLocation
// must not hand back the live pointer UpdateAccessGrantsLocation mutates.
func TestAccessGrantsLocationConcurrentWithUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "GetAccessGrantsLocation races update"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			const accountID = "000000000000"
			b.CreateAccessGrantsInstance(accountID, "")

			loc := b.CreateAccessGrantsLocation(accountID, "s3://", "arn:aws:iam::000000000000:role/initial")

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					got, err := b.GetAccessGrantsLocation(accountID, loc.AccessGrantsLocationID)
					if err != nil {
						continue
					}

					_ = got.IAMRoleArn
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					_, _ = b.UpdateAccessGrantsLocation(
						accountID,
						loc.AccessGrantsLocationID,
						"arn:aws:iam::000000000000:role/updated",
					)
				}
			}()

			wg.Wait()
		})
	}
}
