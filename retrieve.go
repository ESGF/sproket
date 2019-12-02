package sproket

import (
	"io"
	"log"
	"net/http"
	"os"
)

// Get retreives the the URL
func Get(inURL string, dest string) {
	resp, err := http.Get(inURL)
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(dest)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		log.Fatal(err)
	}
}
