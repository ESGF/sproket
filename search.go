package sproket

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

// SearchRes stores the "response" portion of a Solr query result
type SearchRes struct {
	Res Response `json:"response"`
}

// Response stores the number of returned documents and a subset of documents themselves
type Response struct {
	N    int   `json:"numFound"`
	Docs []Doc `json:"docs"`
}

// Doc holds a single search result document, in this case these are ESGF data files
type Doc struct {
	URLs    []string `json:"url"`
	ID      string   `json:"id"`
	Sum     []string `json:"checksum"`
	HTTPURL string
}

// GetSum returns the checksum, since the checksum is stored as a multivalued field
func (d *Doc) GetSum() string {
	return d.Sum[0]
}

// SearchURLs returns a slice of up to "limit" download URLs
func SearchURLs(c *Criteria, sAPI string, skip int, limit int) ([]Doc, int) {
	q := buildQ(c)
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"fields": "id,url,checksum",
		"limit":  fmt.Sprintf("%d", limit),
		"offset": fmt.Sprintf("%d", skip),
	}

	// Perform query
	resp, err := http.Get(Path(sAPI, params))
	if err != nil {
		log.Println(err)
		return nil, 0
	}
	defer resp.Body.Close()

	// Read response body
	body, err := ioutil.ReadAll(resp.Body)

	// Parse response body as JSON
	var result SearchRes
	json.Unmarshal(body, &result)

	// Get downloadable urls
	var docs []Doc
	for _, doc := range result.Res.Docs {
		for _, url := range doc.URLs {
			if strings.Contains(url, "HTTPServer") {
				doc.HTTPURL = strings.Split(url, "|")[0]
			}
		}
		docs = append(docs, doc)
	}

	remaining := result.Res.N - (len(result.Res.Docs) + skip)
	if remaining < 0 {
		remaining = 0
	}
	return docs, remaining
}

// Path builds an http path from host and params
func Path(host string, params map[string]string) string {

	var httpQuery []string
	for key, value := range params {
		param := fmt.Sprintf("%s=%s", key, url.QueryEscape(value))
		httpQuery = append(httpQuery, param)
	}
	query := strings.Join(httpQuery, "&")
	out := fmt.Sprintf("%s?%s", host, query)
	return out
}

func buildQ(c *Criteria) string {
	if len(c.Fields) == 0 {
		return "*:*"
	}
	var matches []string
	for key, value := range c.Fields {
		match := fmt.Sprintf("%s:%s", key, value)
		matches = append(matches, match)
	}
	return strings.Join(matches, " AND ")
}
