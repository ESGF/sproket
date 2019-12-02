package sproket

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type fieldResTop struct {
	Res fieldResMid `json:"response"`
}

type fieldResMid struct {
	Docs []map[string]interface{} `json:"docs"`
}

// SearchFields returns a slice of available fields for a search
func SearchFields(c *Criteria, sAPI string) []string {
	q := buildQ(c)
	params := map[string]string{
		"query":  q,
		"type":   "File",
		"format": "application/solr+json",
		"fields": "*",
		"limit":  "1",
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
	var result fieldResTop
	json.Unmarshal(body, &result)

	var fields []string
	for key := range result.Res.Docs[0] {
		fields = append(fields, key)
	}
	return fields
}
