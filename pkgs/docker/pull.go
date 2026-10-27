package docker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type pullMessage struct {
	ErrorDetail *struct {
		Message string `json:"message"`
	} `json:"errorDetail"`
	Error string `json:"error"`
}

// ConsumePullStream reads an ImagePull response to EOF (closing early cancels
// the pull) and returns the first error the daemon reports in the stream.
func ConsumePullStream(r io.Reader) error {
	dec := json.NewDecoder(r)

	for {
		var msg pullMessage

		err := dec.Decode(&msg)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			_, _ = io.Copy(io.Discard, r)

			return fmt.Errorf("decode image pull stream: %w", err)
		}

		if msg.ErrorDetail != nil && msg.ErrorDetail.Message != "" {
			_, _ = io.Copy(io.Discard, io.MultiReader(dec.Buffered(), r))

			return fmt.Errorf("image pull: %s", msg.ErrorDetail.Message)
		}

		if msg.Error != "" {
			_, _ = io.Copy(io.Discard, io.MultiReader(dec.Buffered(), r))

			return fmt.Errorf("image pull: %s", msg.Error)
		}
	}
}
