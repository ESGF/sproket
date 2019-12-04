package sproket

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type facetRes struct {
	Counts facetCounts `json:"facet_counts"`
}

type facetCounts struct {
	Fields map[string][]interface{} `json:"facet_fields"`
}

// DataNodes returns the data nodes serving the files and the number of files that each data node has
func DataNodes(c *Criteria, sAPI string) map[string]int {
	q := buildQ(c)
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"limit":  "0",
		"facets": "data_node",
	}

	// Perform query
	resp, err := http.Get(Path(sAPI, params))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer resp.Body.Close()

	// Read response body
	body, err := ioutil.ReadAll(resp.Body)

	// Parse response body as JSON
	var result facetRes
	json.Unmarshal(body, &result)

	dataNodeCounts := make(map[string]int)
	var prev string
	for _, value := range result.Counts.Fields["data_node"] {
		if key, ok := value.(string); ok {
			prev = key
		} else if count, ok := value.(float64); ok {
			dataNodeCounts[prev] = int(count)
		}
	}
	return dataNodeCounts
}
