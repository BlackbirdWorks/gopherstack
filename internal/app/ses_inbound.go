package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
)

// sesInboundS3 adapts the S3 backend to ses.InboundObjectStore.
type sesInboundS3 struct{ bk *s3backend.InMemoryBackend }

func (a sesInboundS3) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := a.bk.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	})

	return err
}

func (a sesInboundS3) PutObjectWithMetadata(
	ctx context.Context, bucket, key string, body []byte, meta map[string]string,
) error {
	_, err := a.bk.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Metadata: meta,
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	})

	return err
}

// sesInboundKMS adapts the KMS backend to ses.InboundKMS.
type sesInboundKMS struct{ bk kmsbackend.StorageBackend }

func (a sesInboundKMS) GenerateDataKey(
	ctx context.Context, keyID string, encCtx map[string]string,
) ([]byte, []byte, error) {
	out, err := a.bk.GenerateDataKey(ctx, &kmsbackend.GenerateDataKeyInput{
		KeyID: keyID, KeySpec: "AES_256", EncryptionContext: encCtx,
	})
	if err != nil {
		return nil, nil, err
	}

	return out.Plaintext, out.CiphertextBlob, nil
}

// sesInboundDNS answers TXT, address, MX and PTR lookups from the Route 53 hosted zones.
type sesInboundDNS struct {
	bk *route53backend.InMemoryBackend
}

const (
	sesInboundMaxZones   = 1000 // page size for hosted-zone scan
	sesInboundMaxRecords = 100  // page size for record lookup
	sesInboundMXFields   = 2    // "<preference> <host>"
)

var errSESInboundNoRecord = errors.New("no TXT record")

func (a sesInboundDNS) LookupTXT(_ context.Context, name string) ([]string, error) {
	vals, err := a.records(name, "TXT")
	if err != nil {
		return nil, err
	}

	for i, v := range vals {
		vals[i] = strings.ReplaceAll(strings.Trim(v, `"`), `" "`, "")
	}

	return vals, nil
}

// LookupIP returns the A and AAAA record addresses of name.
func (a sesInboundDNS) LookupIP(_ context.Context, name string) ([]netip.Addr, error) {
	var out []netip.Addr

	for _, typ := range []string{"A", "AAAA"} {
		vals, err := a.records(name, typ)
		if err != nil {
			continue
		}

		for _, v := range vals {
			if ip, perr := netip.ParseAddr(strings.TrimSpace(v)); perr == nil {
				out = append(out, ip)
			}
		}
	}

	if len(out) == 0 {
		return nil, sesbackend.ErrDNSNotFound
	}

	return out, nil
}

// LookupMX returns the exchanger host names of the MX records of name.
func (a sesInboundDNS) LookupMX(_ context.Context, name string) ([]string, error) {
	vals, err := a.records(name, "MX")
	if err != nil {
		return nil, sesbackend.ErrDNSNotFound
	}

	out := make([]string, 0, len(vals))

	for _, v := range vals {
		if f := strings.Fields(v); len(f) == sesInboundMXFields {
			out = append(out, f[1])
		}
	}

	return out, nil
}

// LookupAddr returns the PTR names of ip from the in-addr.arpa and ip6.arpa zones.
func (a sesInboundDNS) LookupAddr(_ context.Context, ip netip.Addr) ([]string, error) {
	vals, err := a.records(reverseName(ip), "PTR")
	if err != nil {
		return nil, sesbackend.ErrDNSNotFound
	}

	return vals, nil
}

func reverseName(ip netip.Addr) string {
	ip = ip.Unmap()

	if ip.Is4() {
		b := ip.As4()

		return fmt.Sprintf("%d.%d.%d.%d.in-addr.arpa", b[3], b[2], b[1], b[0])
	}

	b := ip.As16()
	nibbles := make([]string, 0, len(b)*2)

	for i := len(b) - 1; i >= 0; i-- {
		nibbles = append(nibbles, fmt.Sprintf("%x", b[i]&0xf), fmt.Sprintf("%x", b[i]>>4))
	}

	return strings.Join(nibbles, ".") + ".ip6.arpa"
}

// records returns the values of the typ records at name from the first hosted zone that has any.
func (a sesInboundDNS) records(name, typ string) ([]string, error) {
	fqdn := strings.ToLower(strings.TrimSuffix(name, ".")) + "."

	zones, err := a.bk.ListHostedZones("", sesInboundMaxZones, "", "")
	if err != nil {
		return nil, err
	}

	for _, z := range zones.Data {
		if !strings.HasSuffix(fqdn, strings.ToLower(z.Name)) {
			continue
		}

		if vals := a.zoneRecords(z.ID, fqdn, typ); len(vals) > 0 {
			return vals, nil
		}
	}

	return nil, errSESInboundNoRecord
}

func (a sesInboundDNS) zoneRecords(zoneID, fqdn, typ string) []string {
	pg, err := a.bk.ListResourceRecordSets(zoneID, fqdn, typ, "", sesInboundMaxRecords)
	if err != nil {
		return nil
	}

	var out []string

	for _, rr := range pg.Records {
		if !strings.EqualFold(rr.Name, fqdn) || rr.Type != typ {
			continue
		}

		for _, r := range rr.Records {
			out = append(out, r.Value)
		}
	}

	return out
}

// wireSESInbound connects the SES inbound SMTP listener's S3 and Lambda receipt actions to their backends.
func wireSESInbound(byName map[string]service.Registerable) {
	sesH, ok := byName["SES"].(*sesbackend.Handler)
	if !ok {
		return
	}

	sesBk, ok := sesH.Backend.(*sesbackend.InMemoryBackend)
	if !ok {
		return
	}

	if s3H, s3Ok := byName["S3"].(*s3backend.S3Handler); s3Ok {
		if bk, bkOk := s3H.Backend.(*s3backend.InMemoryBackend); bkOk {
			sesBk.SetInboundObjectStore(sesInboundS3{bk: bk})
		}
	}

	if kH, kOk := byName["KMS"].(*kmsbackend.Handler); kOk {
		sesBk.SetInboundKMS(sesInboundKMS{bk: kH.Backend})
	}

	if rH, rOk := byName["Route53"].(*route53backend.Handler); rOk {
		if bk, bkOk := rH.Backend.(*route53backend.InMemoryBackend); bkOk {
			sesBk.SetInboundTXTResolver(sesInboundDNS{bk: bk})
		}
	}

	if lH, lOk := byName["Lambda"].(*lambdabackend.Handler); lOk {
		if bk, bkOk := lH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			sesBk.SetInboundLambdaInvoker(bk)
		}
	}
}
