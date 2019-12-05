package sproket

// Search holds the ESGF search API to use and criteria to apply
type Search struct {
	API              string            `json:"search_api"`
	Fields           map[string]string `json:"fields"`
	DataNodePriority []string          `json:"data_node_priority"`
}
