package s3

import (
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const (
	// objectLambdaPresignExpiry is not documented by AWS for inputS3Url.
	objectLambdaPresignExpiry = 5 * time.Minute
	fallbackAccessKeyID       = "AKIAGOPHERSTACK"
	assumedRoleARNParts       = 2
)

// presignObjectLambdaInput SigV4-presigns rawURL (host header only signed) as the caller, per
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/olap-event-context.html ("inputS3Url").
func (h *S3Handler) presignObjectLambdaInput(r *http.Request, method, rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	accessKey := awsmeta.AccessKeyID(r.Context())
	if accessKey == "" {
		accessKey = fallbackAccessKeyID
	}

	now := time.Now().UTC()
	date := now.Format("20060102")
	region := regionFromRequest(r, h.DefaultRegion)
	scope := strings.Join([]string{date, region, "s3", "aws4_request"}, "/")

	q := u.Query()
	q.Set("X-Amz-Algorithm", presignedAlgorithm)
	q.Set("X-Amz-Credential", accessKey+"/"+scope)
	q.Set("X-Amz-Date", now.Format(presignedDateFormat))
	q.Set("X-Amz-Expires", strconv.Itoa(int(objectLambdaPresignExpiry/time.Second)))
	q.Set("X-Amz-SignedHeaders", "host")
	u.RawQuery = q.Encode()

	signed := &http.Request{Method: method, URL: u, Host: u.Host, Header: http.Header{}}
	canonical := h.buildPresignCanonicalRequest(signed, []string{"host"})
	toSign := strings.Join([]string{presignedAlgorithm, now.Format(presignedDateFormat), scope, hexSHA256(canonical)}, "\n")
	sig := hex.EncodeToString(hmacSHA256Bytes(derivePresignSigningKey(h.PresignSecret, date, region, "s3"), toSign))

	q.Set("X-Amz-Signature", sig)
	u.RawQuery = q.Encode()

	return u.String()
}

// objectLambdaSessionContext builds userIdentity.sessionContext for assumed-role callers.
func objectLambdaSessionContext(p *awsmeta.Principal, region string, now time.Time) *objectLambdaSession {
	if p.Kind != awsmeta.PrincipalKindAssumedRole {
		return nil
	}

	// arn:aws:sts::<acct>:assumed-role/<role>/<session>
	_, res, _ := strings.Cut(p.Arn, ":assumed-role/")
	roleName, _, _ := strings.Cut(res, "/")

	return &objectLambdaSession{
		Attributes: objectLambdaSessionAttrs{
			MFAAuthenticated: "false",
			CreationDate:     now.Format(time.UnixDate),
		},
		SessionIssuer: objectLambdaSessionIssuer{
			Type:        "Role",
			PrincipalID: p.UserID,
			ARN:         arn.BuildGlobal("iam", region, p.AccountID, "role/"+roleName),
			AccountID:   p.AccountID,
			UserName:    roleName,
		},
	}
}
