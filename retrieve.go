package sproket

import (
	"errors"
	"io"
	"net/http"
	"os"
)

// Get retreives the the URL
func Get(inURL string, dest string) error {
	resp, err := http.Get(inURL)
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
