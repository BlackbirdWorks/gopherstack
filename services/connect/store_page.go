package connect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func restroutePaginate[T any](all []T, token string, maxResults int32, def, maxAllowed int) (page.Page[T], error) {
	p, err := restroute.Paginate(all, token, int(maxResults), def, maxAllowed)
	if err != nil {
		return p, invalidParamErr("%v", err)
	}

	return p, nil
}
