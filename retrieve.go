package sproket

import (
	"errors"
	"fmt"
	"io"
	"net/http"
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

	// Write to destination
	nBytes, err := io.Copy(dest, resp.Body)
	if err != nil {
		return err
	}
	if resp.ContentLength != -1 && nBytes != resp.ContentLength {
		return fmt.Errorf("response size mismatch: %d != %d", nBytes, resp.ContentLength)
	}
	return nil
}
