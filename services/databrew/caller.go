package databrew

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// stampModifiedBy records the calling principal as the last modifier; an unresolved caller leaves dst unchanged.
func stampModifiedBy(ctx context.Context, dst *string) {
	if arn := awsmeta.CallerArn(ctx); arn != "" {
		*dst = arn
	}
}
