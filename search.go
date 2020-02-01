package sproket

import (
	"bytes"
	"encoding/json"
	"fmt"
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
func (s *Search) SearchURLs(skip int, limit int) ([]Doc, int) {
	q := s.buildQ()
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"fields": "instance_id,url,checksum,data_node,checksum_type",
		"limit":  fmt.Sprintf("%d", limit),
		"offset": fmt.Sprintf("%d", skip),
	}

	body, err := s.performSearch(params)
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

func (s *Search) performSearch(params map[string]string) ([]byte, error) {

	// Build the search path
	values := url.Values{}
	for key, value := range params {
		values.Add(key, value)
	}
	query := values.Encode()
	path := fmt.Sprintf("%s?%s", s.API, query)

	// Perform query
	buff := bytes.Buffer{}
	err := s.Get(path, &buff)
	return buff.Bytes(), err
}

func (s *Search) buildQ() string {
	if len(s.Fields) == 0 {
		return "*:*"
	}
	var matches []string
	for key, value := range s.Fields {
		match := fmt.Sprintf("%s:(%s)", key, value)
		matches = append(matches, match)
	}
	return strings.Join(matches, " AND ")
}
