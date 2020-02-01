package sproket

import "net/http"

// Search holds the ESGF search API to use and criteria to apply
type Search struct {
	API              string            `json:"search_api"`
	Fields           map[string]string `json:"fields"`
	DataNodePriority []string          `json:"data_node_priority"`
	Agent            string
	HTTPClient       *http.Client
}
