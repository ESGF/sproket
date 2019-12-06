package sproket

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	URLs       []string `json:"url"`
	InstanceID string   `json:"instance_id"`
	DataNode   string   `json:"data_node"`
	Sum        []string `json:"checksum"`
	SumType    []string `json:"checksum_type"`
	HTTPURL    string
}

// GetSum returns the checksum, since the checksum is stored as a multivalued field
func (d *Doc) GetSum() string {
	if len(d.Sum) != 1 {
		return ""
	}
	return d.Sum[0]
}

// GetSumType returns the checksum, since the checksum is stored as a multivalued field
func (d *Doc) GetSumType() string {
	if len(d.Sum) != 1 {
		return ""
	}
	return d.SumType[0]
}

// SearchURLs returns a slice of up to "limit" download URLs
func SearchURLs(s *Search, skip int, limit int) ([]Doc, int) {
	q := buildQ(s)
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"fields": "instance_id,url,checksum,data_node,checksum_type",
		"limit":  fmt.Sprintf("%d", limit),
		"offset": fmt.Sprintf("%d", skip),
	}

	body, err := performSearch(s.API, params)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}

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

func performSearch(api string, params map[string]string) ([]byte, error) {

	// Perform query
	resp, err := http.Get(Path(api, params))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
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

func buildQ(s *Search) string {
	if len(s.Fields) == 0 {
		return "*:*"
	}
	var matches []string
	for key, value := range s.Fields {
		match := fmt.Sprintf("%s:%s", key, value)
		matches = append(matches, match)
	}
	return strings.Join(matches, " AND ")
}
