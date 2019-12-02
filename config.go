package sproket

// Downloads holds exact Urls, *File* Ids or Search criteria
type Downloads struct {
	SAPI string     `json:"search_api"`
	Reqs []Criteria `json:"criteria"`
}

// Criteria is a group of ANDed requirements
type Criteria struct {
	Fields   map[string]string `json:"fields"`
	Start    int               `json:"skip"`
	Disabled bool              `json:"disabled"`
}
