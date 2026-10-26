package bad

import "bytes"

type policy struct{ Etag []byte }

type backend struct{ cur policy }

func (b *backend) SetIamPolicy(in policy) error {
	if !bytes.Equal(in.Etag, b.cur.Etag) {
		return nil
	}

	return nil
}
