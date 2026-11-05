package sparql

import (
	"crypto/md5" //nolint:gosec // SPARQL MD5() is a required builtin over caller-supplied literals, not a credential
	"crypto/sha1" //nolint:gosec // SPARQL SHA1() is a required builtin over caller-supplied literals, not a credential
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
)

func hashFn(h func() hash.Hash) builtin {
	return arity(1, func(_ *evaluator, a []Term) (Term, error) {
		s, _, ok := strOf(a[0])
		if !ok {
			return Term{}, errType
		}

		w := h()
		_, _ = w.Write([]byte(s))

		return Literal(hex.EncodeToString(w.Sum(nil))), nil
	})
}

func addHashFuncs(t map[string]builtin) {
	t["MD5"] = hashFn(md5.New)
	t["SHA1"] = hashFn(sha1.New)
	t["SHA256"] = hashFn(sha256.New)
	t["SHA384"] = hashFn(sha512.New384)
	t["SHA512"] = hashFn(sha512.New)
}
