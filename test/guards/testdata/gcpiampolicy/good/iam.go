package good

import "github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"

type backend struct{}

func (b *backend) SetIamPolicy(in *int) error {
	_ = gcpiampolicy.EmptyEtag()
	_ = in.Etag

	return nil
}
