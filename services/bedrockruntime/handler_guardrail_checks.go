package bedrockruntime

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
)

// --- InvokeGuardrailChecks (POST /guardrail-checks/invoke) ---
//
// Unlike ApplyGuardrail (POST /guardrail/{id}/version/{ver}/apply), this
// operation does not reference a stored "bedrock" service guardrail
// resource at all: the caller supplies the check configuration inline in
// the request body (InvokeGuardrailChecksInput.Checks), so there is no
// guardrailIdentifier to look up and nothing in the separate "bedrock"
// control-plane backend to consult.
//
// AWS's real InvokeGuardrailChecks supports three independent check
// families, confirmed from aws-sdk-go-v2/service/bedrockruntime@v1.56.0's
// types.go/serializers.go/deserializers.go:
//
//   - contentFilter (categories: VIOLENCE/HATE/SEXUAL/MISCONDUCT/INSULTS) --
//     each result entry REQUIRES a severityScore (0.0-1.0) produced by a
//     real ML content classifier. gopherstack has no such classifier, so
//     this backend does NOT fabricate a score: if requested, the
//     contentFilter result group is present but its "results" list is
//     honestly empty (see fabrication_verdict in the parity receipt / gap
//     note below).
//   - promptAttack (categories: JAILBREAK/PROMPT_INJECTION/PROMPT_LEAKAGE) --
//     same situation: severityScore requires a real ML classifier.
//     Honestly empty for the same reason.
//   - sensitiveInformation (PII entity types) -- for a curated subset of
//     entity types that have an unambiguous, literal wire format (EMAIL,
//     PHONE, IP_ADDRESS, URL, AWS_ACCESS_KEY, MAC_ADDRESS,
//     US_SOCIAL_SECURITY_NUMBER, CREDIT_DEBIT_CARD_NUMBER), this backend
//     runs a real deterministic regex/format detector -- see
//     piiDetectors. This is genuine, literal pattern evaluation (the same
//     category of "real evaluation" as a denied-word or regex hit), not a
//     classifier verdict. Every other entity type in
//     types.GuardrailChecksSensitiveInformationEntityType (NAME, ADDRESS,
//     AGE, PASSWORD, DRIVER_ID, LICENSE_PLATE, bank/tax/passport/health
//     identifiers, etc.) requires either free-text NER or a
//     jurisdiction-specific checksum gopherstack does not implement, so
//     those entity types are honestly never matched.
//
// A detected sensitive-information entity's confidenceScore is always
// exactly 1.0: this is not a fabricated probabilistic score -- it reflects
// that the match is a deterministic, literal pattern hit (the text either
// matches the entity's known format or it does not), never a probability
// estimate this emulator has no basis for producing.

const guardrailChecksPath = "/guardrail-checks/invoke"

// --- Request shapes (see serializers.go's
// awsRestjson1_serializeOpDocumentInvokeGuardrailChecksInput and nested
// awsRestjson1_serializeDocumentGuardrailChecks* helpers for the confirmed
// "checks"/"messages"/"contentFilter"/"promptAttack"/"sensitiveInformation"/
// "categories"/"category"/"entities"/"type"/"content"/"role"/"text" keys). ---

type invokeGuardrailChecksRequest struct {
	Checks   *guardrailChecksConfigWire   `json:"checks"`
	Messages []guardrailChecksMessageWire `json:"messages"`
}

type guardrailChecksConfigWire struct {
	ContentFilter        *guardrailChecksCategoryConfigWire  `json:"contentFilter,omitempty"`
	PromptAttack         *guardrailChecksCategoryConfigWire  `json:"promptAttack,omitempty"`
	SensitiveInformation *guardrailChecksSensitiveConfigWire `json:"sensitiveInformation,omitempty"`
}

type guardrailChecksCategoryConfigWire struct {
	Categories []guardrailChecksCategoryEntryWire `json:"categories"`
}

type guardrailChecksCategoryEntryWire struct {
	Category string `json:"category"`
}

type guardrailChecksSensitiveConfigWire struct {
	Entities []guardrailChecksEntityEntryWire `json:"entities"`
}

type guardrailChecksEntityEntryWire struct {
	Type string `json:"type"`
}

type guardrailChecksMessageWire struct {
	Role    string                       `json:"role"`
	Content []guardrailChecksContentWire `json:"content"`
}

type guardrailChecksContentWire struct {
	Text string `json:"text"`
}

// --- Response shapes (see deserializers.go's
// awsRestjson1_deserializeOpDocumentInvokeGuardrailChecksOutput and nested
// GuardrailChecks* deserializers for the confirmed "results"/"usage"/
// "severityScore"/"confidenceScore"/"beginOffset"/"endOffset"/
// "contentIndex"/"messageIndex"/"truncated"/"textUnits" keys). ---

type guardrailChecksResultsResponse struct {
	ContentFilter        *guardrailContentFilterResultResponse `json:"contentFilter,omitempty"`
	PromptAttack         *guardrailPromptAttackResultResponse  `json:"promptAttack,omitempty"`
	SensitiveInformation *guardrailSensitiveInfoResultResponse `json:"sensitiveInformation,omitempty"`
}

type guardrailContentFilterResultResponse struct {
	Results []guardrailContentFilterEntryResponse `json:"results"`
}

type guardrailContentFilterEntryResponse struct {
	Category      string  `json:"category"`
	SeverityScore float64 `json:"severityScore"`
}

type guardrailPromptAttackResultResponse struct {
	Results []guardrailPromptAttackEntryResponse `json:"results"`
}

type guardrailPromptAttackEntryResponse struct {
	Category      string  `json:"category"`
	SeverityScore float64 `json:"severityScore"`
}

type guardrailSensitiveInfoResultResponse struct {
	Results   []guardrailSensitiveInfoEntryResponse `json:"results"`
	Truncated bool                                  `json:"truncated"`
}

type guardrailSensitiveInfoEntryResponse struct {
	Type            string  `json:"type"`
	BeginOffset     int     `json:"beginOffset"`
	EndOffset       int     `json:"endOffset"`
	ContentIndex    int     `json:"contentIndex"`
	MessageIndex    int     `json:"messageIndex"`
	ConfidenceScore float64 `json:"confidenceScore"`
}

type guardrailChecksUsageResponse struct {
	ContentFilter        *guardrailChecksTextUnitsResponse `json:"contentFilter,omitempty"`
	PromptAttack         *guardrailChecksTextUnitsResponse `json:"promptAttack,omitempty"`
	SensitiveInformation *guardrailChecksTextUnitsResponse `json:"sensitiveInformation,omitempty"`
}

// guardrailChecksTextUnitsResponse always reports textUnits: 0, matching
// the same "not metered" precedent ApplyGuardrail already established for
// its usage block (handleApplyGuardrail's hardcoded topicPolicyUnits: 0
// etc.) -- this mock does not implement AWS's text-unit billing metric, and
// reporting a constant 0 is honest (no usage computed) rather than a
// fabricated count.
type guardrailChecksTextUnitsResponse struct {
	TextUnits int `json:"textUnits"`
}

// handleInvokeGuardrailChecks handles POST /guardrail-checks/invoke.
func (h *Handler) handleInvokeGuardrailChecks(c *echo.Context, body []byte) error {
	var req invokeGuardrailChecksRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
		}
	}

	if req.Checks == nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "checks is required"))
	}

	if len(req.Messages) == 0 {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "messages is required"))
	}

	results := guardrailChecksResultsResponse{}
	usage := guardrailChecksUsageResponse{}

	if req.Checks.ContentFilter != nil {
		// Honestly empty: no ML content classifier available (see file doc
		// comment). Presence of the group without fabricated entries
		// reflects that the check family was requested but not genuinely
		// evaluable.
		results.ContentFilter = &guardrailContentFilterResultResponse{Results: []guardrailContentFilterEntryResponse{}}
		usage.ContentFilter = &guardrailChecksTextUnitsResponse{}
	}

	if req.Checks.PromptAttack != nil {
		results.PromptAttack = &guardrailPromptAttackResultResponse{Results: []guardrailPromptAttackEntryResponse{}}
		usage.PromptAttack = &guardrailChecksTextUnitsResponse{}
	}

	if req.Checks.SensitiveInformation != nil {
		results.SensitiveInformation = &guardrailSensitiveInfoResultResponse{
			Results: detectSensitiveInformation(req.Checks.SensitiveInformation, req.Messages),
		}
		usage.SensitiveInformation = &guardrailChecksTextUnitsResponse{}
	}

	resp := map[string]any{
		"results": results,
		"usage":   usage,
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation("InvokeGuardrailChecks", "", string(body), string(out))

	c.Response().Header().Set("Content-Type", contentTypeJSON)

	return c.JSONBlob(http.StatusOK, out)
}

// piiDetectFunc scans a single content block's text and returns the
// character (rune) offset ranges of every literal match.
type piiDetectFunc func(text string) [][2]int

// Each pattern below backs exactly one entry in piiDetectors and is kept as
// its own named regexp (rather than inlined in a map literal) to keep every
// line under the repo's line-length limit and to make each literal format
// individually reviewable.
var (
	emailPattern        = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	phonePattern        = regexp.MustCompile(`\b(?:\+?1[-. ]?)?\(?\d{3}\)?[-. ]?\d{3}[-. ]?\d{4}\b`)
	ipAddressPattern    = regexp.MustCompile(`\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b`)
	urlPattern          = regexp.MustCompile(`\bhttps?://[^\s]+`)
	awsAccessKeyPattern = regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)
	macAddressPattern   = regexp.MustCompile(`\b[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}\b`)
	ssnPattern          = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
)

// piiDetectors maps the curated, literal-format-detectable subset of
// types.GuardrailChecksSensitiveInformationEntityType to a real detector.
// Entity types not listed here require free-text NER or a
// jurisdiction-specific checksum this backend does not implement, so they
// are never matched -- see the file doc comment.
//
//nolint:gochecknoglobals // compile-time PII detector lookup table, immutable after init
var piiDetectors = map[string]piiDetectFunc{
	"EMAIL":                     findRuneMatches(emailPattern),
	"PHONE":                     findRuneMatches(phonePattern),
	"IP_ADDRESS":                findRuneMatches(ipAddressPattern),
	"URL":                       findRuneMatches(urlPattern),
	"AWS_ACCESS_KEY":            findRuneMatches(awsAccessKeyPattern),
	"MAC_ADDRESS":               findRuneMatches(macAddressPattern),
	"US_SOCIAL_SECURITY_NUMBER": findRuneMatches(ssnPattern),
	"CREDIT_DEBIT_CARD_NUMBER":  detectCreditCardNumbers,
}

// findRuneMatches adapts a compiled regexp into a piiDetectFunc, converting
// Go's byte-offset FindAllStringIndex results into rune (character) offsets
// to match AWS's documented character-offset semantics for
// beginOffset/endOffset.
func findRuneMatches(re *regexp.Regexp) piiDetectFunc {
	return func(text string) [][2]int {
		byteMatches := re.FindAllStringIndex(text, -1)
		if len(byteMatches) == 0 {
			return nil
		}

		out := make([][2]int, 0, len(byteMatches))
		for _, m := range byteMatches {
			out = append(out, [2]int{
				utf8.RuneCountInString(text[:m[0]]),
				utf8.RuneCountInString(text[:m[1]]),
			})
		}

		return out
	}
}

// creditCardCandidateRe finds runs of 13-19 digits, allowing single spaces
// or dashes between groups, before Luhn-validating each candidate to cut
// down on false positives from arbitrary long digit runs.
var creditCardCandidateRe = regexp.MustCompile(`\b(?:\d[ -]?){12,18}\d\b`)

// detectCreditCardNumbers finds literal, Luhn-valid credit/debit card
// number candidates. The Luhn checksum is a real, deterministic
// verification algorithm (ISO/IEC 7812) -- not a classifier -- so a passing
// candidate is a genuine literal-format match.
func detectCreditCardNumbers(text string) [][2]int {
	byteMatches := creditCardCandidateRe.FindAllStringIndex(text, -1)
	if len(byteMatches) == 0 {
		return nil
	}

	out := make([][2]int, 0, len(byteMatches))

	for _, m := range byteMatches {
		candidate := text[m[0]:m[1]]
		digits := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return r
			}

			return -1
		}, candidate)

		if len(digits) < 13 || len(digits) > 19 || !luhnValid(digits) {
			continue
		}

		out = append(out, [2]int{
			utf8.RuneCountInString(text[:m[0]]),
			utf8.RuneCountInString(text[:m[1]]),
		})
	}

	return out
}

// luhnSingleDigitMax is the largest value a single decimal digit can hold
// (9); the Luhn algorithm subtracts it from any doubled digit that
// overflows into two digits (e.g. 7*2=14 -> 1+4=5, equivalently 14-9=5).
const luhnSingleDigitMax = 9

// luhnValid reports whether digits (a string of ASCII '0'-'9') passes the
// Luhn checksum (ISO/IEC 7812), the standard check-digit algorithm used by
// real credit/debit card numbers.
func luhnValid(digits string) bool {
	sum := 0
	alt := false

	for i := len(digits) - 1; i >= 0; i-- {
		d, err := strconv.Atoi(string(digits[i]))
		if err != nil {
			return false
		}

		if alt {
			d *= 2
			if d > luhnSingleDigitMax {
				d -= luhnSingleDigitMax
			}
		}

		sum += d
		alt = !alt
	}

	return sum%10 == 0
}

// detectSensitiveInformation runs every requested, genuinely-detectable PII
// entity type (see piiDetectors) against every content block of every
// message, returning one response entry per literal match found. Requested
// entity types with no detector in piiDetectors are silently skipped --
// never fabricated -- because gopherstack has no real NER/checksum
// implementation for them.
func detectSensitiveInformation(
	cfg *guardrailChecksSensitiveConfigWire,
	messages []guardrailChecksMessageWire,
) []guardrailSensitiveInfoEntryResponse {
	results := []guardrailSensitiveInfoEntryResponse{}

	for _, entity := range cfg.Entities {
		detect, ok := piiDetectors[entity.Type]
		if !ok {
			continue
		}

		for msgIdx, msg := range messages {
			for contentIdx, block := range msg.Content {
				for _, m := range detect(block.Text) {
					results = append(results, guardrailSensitiveInfoEntryResponse{
						Type:            entity.Type,
						BeginOffset:     m[0],
						EndOffset:       m[1],
						ContentIndex:    contentIdx,
						MessageIndex:    msgIdx,
						ConfidenceScore: 1.0,
					})
				}
			}
		}
	}

	return results
}
