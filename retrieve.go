package sproket

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

// Get sets the User-Agent header, performs the GET and writes to the specified dest file
func Get(inURL string, dest string, agent string) error {

	// Setup http client and set the User-Agent header
	client := &http.Client{}
	req, err := http.NewRequest("GET", inURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", agent)

	// Perform the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	// Create the destination file
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write to destination
	nBytes, err := io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	if nBytes == 0 {
		return fmt.Errorf("did not write any data to %s", dest)
	}
	return nil
}
