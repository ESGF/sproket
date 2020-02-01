package sproket

import (
	"encoding/json"
	"fmt"
)

type facetRes struct {
	Counts facetCounts `json:"facet_counts"`
}

type facetCounts struct {
	Fields map[string][]interface{} `json:"facet_fields"`
}

// Facet returns the values available for the provided field and the number of files that each value has
func (s *Search) Facet(field string) map[string]int {
	q := s.buildQ()
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"limit":  "0",
		"facets": field,
	}

	body, err := s.performSearch(params)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	// Parse response body as JSON
	var result facetRes
	json.Unmarshal(body, &result)

	valueCounts := make(map[string]int)
	var prev string
	for _, value := range result.Counts.Fields[field] {
		if key, ok := value.(string); ok {
			prev = key
		} else if count, ok := value.(float64); ok {
			valueCounts[prev] = int(count)
		}
	}
	return valueCounts
}
