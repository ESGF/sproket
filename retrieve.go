package sproket

import (
	"errors"
	"io"
	"net/http"
	"os"
)

// Get sets the User-Agent header, performs the GET and writes to the specified dest file
func Get(inURL string, dest string, agent string) error {

	client := &http.Client{}
	req, err := http.NewRequest("GET", inURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", agent)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return err
	}
	return nil
}
