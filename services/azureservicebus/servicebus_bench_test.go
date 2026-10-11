package azureservicebus_test

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func benchSB(b *testing.B, n int) (*azureservicebus.InMemoryBackend, azureservicebus.EntityRef) {
	b.Helper()

	be := azureservicebus.NewInMemoryBackend()
	if _, err := be.CreateQueue("q"); err != nil {
		b.Fatal(err)
	}

	ref := azureservicebus.EntityRef{Queue: "q"}

	for i := range n {
		if _, err := be.Send(ref, azureservicebus.NewMessage{
			MessageID: fmt.Sprintf("m%d", i), Body: []byte("hello world payload"),
		}); err != nil {
			b.Fatal(err)
		}
	}

	return be, ref
}

func BenchmarkSBSend(b *testing.B) {
	be, ref := benchSB(b, 0)
	msg := azureservicebus.NewMessage{Body: []byte("hello world payload"), Label: "l"}
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		_, _ = be.Send(ref, msg)
		if i++; i%10000 == 0 {
			b.StopTimer()
			be, ref = benchSB(b, 0)
			b.StartTimer()
		}
	}
}

func BenchmarkSBReceiveComplete10k(b *testing.B) {
	be, ref := benchSB(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		m, err := be.PeekLock(ref, false, time.Minute)
		if err != nil {
			b.StopTimer()
			be, ref = benchSB(b, 10000)
			b.StartTimer()

			continue
		}

		if cErr := be.Complete(ref, false, m.MessageID, m.LockToken); cErr != nil {
			b.Fatal(cErr)
		}
	}
}

func BenchmarkSBInflight10k(b *testing.B) {
	setup := func() (*azureservicebus.InMemoryBackend, azureservicebus.EntityRef, []azureservicebus.MessageInfo) {
		be, ref := benchSB(b, 10000)
		locked := make([]azureservicebus.MessageInfo, 0, 9000)

		for range 9000 {
			m, err := be.PeekLock(ref, false, time.Hour)
			if err != nil {
				b.Fatal(err)
			}

			locked = append(locked, m)
		}

		return be, ref, locked
	}

	be, ref, locked := setup()

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		m, err := be.PeekLock(ref, false, time.Minute)
		if err != nil {
			b.StopTimer()
			be, ref, locked = setup()
			b.StartTimer()

			continue
		}

		_ = be.Complete(ref, false, m.MessageID, m.LockToken)
		l := locked[len(locked)-1-(i%3000)]
		_ = be.Complete(ref, false, l.MessageID, "wrong")
		i++
	}
}

func BenchmarkSBVerifySAS(b *testing.B) {
	exp := time.Now().Add(time.Hour).Unix()
	sig := azureservicebus.SignSAS("http://h/q", exp, azureservicebus.DefaultKeyValue)
	hdr := "SharedAccessSignature sr=http%3A%2F%2Fh%2Fq&sig=" + url.QueryEscape(
		sig,
	) + fmt.Sprintf(
		"&se=%d&skn=Root",
		exp,
	)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		auth, _ := azureservicebus.ParseSASAuthorization(hdr)
		if !azureservicebus.VerifySAS(auth, azureservicebus.DefaultKeyValue, time.Now()) {
			b.Fatal("verify failed")
		}
	}
}
