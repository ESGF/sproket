package sproket

import (
	"encoding/json"
	"fmt"
)

type fieldResTop struct {
	Res fieldResMid `json:"response"`
}

type fieldResMid struct {
	Docs []map[string]interface{} `json:"docs"`
}

// GetFields returns a slice of available fields for a search
func (s *Search) GetFields() []string {
	q := s.buildQ()
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"fields": "*",
		"limit":  "1",
	}

	body, err := s.performSearch(params)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	// Parse response body as JSON
	var result fieldResTop
	json.Unmarshal(body, &result)

	// If no result was found
	if len(result.Res.Docs) != 1 {
		return nil
	}

	var fields []string
	for key := range result.Res.Docs[0] {
		fields = append(fields, key)
	}
	return fields
}
