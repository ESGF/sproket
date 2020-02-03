package sproket

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// Get sets the User-Agent header, performs the GET and writes to the specified dest io writer
func (s *Search) Get(inURL string, dest io.Writer) error {

	// Setup http client and set the User-Agent header
	req, err := http.NewRequest("GET", inURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", s.Agent)

	// Perform the HTTP request
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	// Grab the expected size
	expectedSize, err := strconv.ParseInt(resp.Header.Get("content-length"), 10, 64)
	if err != nil {
		expectedSize = int64(-1)
	}

	// Write to destination
	nBytes, err := io.Copy(dest, resp.Body)
	if err != nil {
		return err
	}
	if expectedSize != -1 && nBytes != expectedSize {
		return fmt.Errorf("response size mismatch: %d != %d", nBytes, expectedSize)
	}
	return nil
}
