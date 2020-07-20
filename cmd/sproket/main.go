package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sproket"
	"strings"
	"sync"
)

// VERSION is the current version of sproket
var VERSION = "v0.2.14"

// AGENT sets the User-Agent field in the HTTP requests
var AGENT = fmt.Sprintf("sproket/%s", VERSION)

type config struct {
	conf             string
	outDir           string
	valuesFor        string
	parallel         int
	noDownload       bool
	urlsOnly         bool
	verbose          bool
	confirm          bool
	count            bool
	noVerify         bool
	version          bool
	fieldKeys        bool
	displayDataNodes bool
	softDataNode     bool
	unsafe           bool
	search           sproket.Search
}

func (args *config) Init() error {

	// Load config file
	fileBytes, err := ioutil.ReadFile(args.conf)
	if err != nil {
		return fmt.Errorf("%s not found", args.conf)
	}

	// Validate JSON
	if !(json.Valid(fileBytes)) {
		return fmt.Errorf("%s does not contain valid JSON", args.conf)
	}

	// Load JSON config
	json.Unmarshal(fileBytes, &args.search)
	if args.search.API == "" {
		return fmt.Errorf("search_api is required parameter in config file")
	}

	// Hard set special fields
	args.search.Fields["replica"] = "*"
	args.search.Fields["data_node"] = "*"
	if !(args.unsafe) {
		args.search.Fields["retracted"] = "false"
		args.search.Fields["latest"] = "true"
	}

	args.softDataNode = (len(args.search.DataNodePriority) != 0)

	// Configure HTTP settings
	args.search.Agent = AGENT
	args.search.HTTPClient = &http.Client{}

	if _, err := os.Stat(args.outDir); os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist", args.outDir)
	}
	return nil
}

func getHasher(dest string, remoteSum string, remoteSumType string) (hash.Hash, error) {
	if remoteSumType == "" || remoteSum == "" {
		return nil, fmt.Errorf("could not retrieve checksum for %s", dest)
	}
	switch remoteSumType {
	case "MD5":
		return md5.New(), nil
	case "SHA256":
		return sha256.New(), nil
	default:
		return nil, fmt.Errorf("unrecognized checksum_type: %s", remoteSumType)
	}
}

func check(dest string, remoteSum string, remoteSumType string) error {
	hash, err := getHasher(dest, remoteSum, remoteSumType)
	if err != nil {
		return err
	}
	f, err := os.Open(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(hash, f); err != nil {
		return err
	}
	res := fmt.Sprintf("%x", hash.Sum(nil))
	if res != remoteSum {
		return fmt.Errorf("checksum verification failure for %s", dest)
	}
	return nil
}

func getData(id int, inDocs <-chan sproket.Doc, waiter *sync.WaitGroup, args *config) {
	defer waiter.Done()
	for doc := range inDocs {
		// Report download when verbose
		if args.verbose {
			fmt.Printf("%d: download %s\n", id, doc.HTTPURL)
		}
		// Report URLs only, if applicable
		if args.urlsOnly {
			fmt.Println(doc.HTTPURL)
		} else if args.noDownload {
			// Do nothing in no download, except report if verbose
			if args.verbose {
				fmt.Printf("%d: no download\n", id)
			}
		} else { // Do the download
			// Build filenames
			destName := filepath.Join(args.outDir, fmt.Sprintf("%s.part", doc.InstanceID))
			finalDestName := filepath.Join(args.outDir, doc.InstanceID)

			// Check if file is already present and correct
			if _, err := os.Stat(finalDestName); err == nil {
				err = check(finalDestName, doc.GetSum(), doc.GetSumType())
				// Go to next download if everything checks out
				if err == nil {
					if args.verbose {
						fmt.Printf("%d: %s already present and verified, no download\n", id, finalDestName)
					}
					continue
				}
			}

			// Create the destination file
			fileWriter, err := os.Create(destName)
			if err != nil {
				fmt.Printf("%d: unable to create %s: %s\n", id, destName, err)
				continue
			}
			defer fileWriter.Close()

			// Create destination writer and set the default writer
			var dest io.Writer
			dest = fileWriter

			// Create hash for potential later use
			h, hashErr := getHasher(finalDestName, doc.GetSum(), doc.GetSumType())
			if hashErr != nil && !(args.noVerify) {
				fmt.Printf("%d: hash warning: %s\n", id, hashErr)
			} else if !(args.noVerify) {
				// Write to both the file and the hash in memory, not parallel though
				dest = io.MultiWriter(h, fileWriter)
			}

			// Perform download
			err = args.search.Get(doc.HTTPURL, dest)
			fileWriter.Close()
			if err != nil {
				fmt.Printf("%d: an error occurred during download of %s:\n\t%s\n", id, doc.HTTPURL, err)
				continue
			}

			// Verify checksum, if available and desired
			if hashErr == nil && !(args.noVerify) {
				verified := (fmt.Sprintf("%x", h.Sum(nil)) == doc.GetSum())
				if !(verified) {
					fmt.Printf("checksum verification failure for %s", finalDestName)
					continue
				} else if args.verbose {
					fmt.Printf("%d: verified %s\n", id, destName)
				}
			}

			// Rename the file to indicate it is verified
			if hashErr == nil || args.noVerify {
				err = os.Rename(destName, finalDestName)
				if err != nil {
					fmt.Println(err)
					continue
				} else if args.verbose {
					fmt.Printf("%d: removed postfix %s\n", id, finalDestName)
				}
			}
		}
	}
}

func getBySearch(args *config) {

	// Count original files, only files with "replica: false" entries present in the index will be downloaded
	args.search.Fields["replica"] = "false"
	if args.verbose {
		fmt.Println(args.search)
	}
	_, n := args.search.SearchURLs(0, 0)
	if !(args.urlsOnly) {
		fmt.Printf("found %d files for download\n", n)
	}
	if args.count || n == 0 {
		return
	}
	warnCount := 100
	if !(args.confirm) && n > warnCount {
		fmt.Printf("too many files (%d > %d): confirm larger download by specifying the -y option or refine search criteria\n", n, warnCount)
		return
	}

	// Check if the soft data node list will even matter
	dataNodeMatches := make(map[string]bool)
	if args.softDataNode {
		// Check for any matching replica data nodes in data node priority list
		args.search.Fields["replica"] = "true"
		dataNodes := args.search.Facet("data_node")
		for dataNode := range dataNodes {
			for _, preferedDataNode := range args.search.DataNodePriority {
				if dataNode == preferedDataNode {
					dataNodeMatches[dataNode] = true
				}
			}
		}
		if args.verbose {
			fmt.Println("matching data nodes:")
			fmt.Println(dataNodeMatches)
		}
		if len(dataNodeMatches) == 0 {
			args.softDataNode = false
		}
		// Reset replica to false
		args.search.Fields["replica"] = "false"
	}

	// Setup download workers in case data node does not matter and for later
	docChan := make(chan sproket.Doc)
	waiter := sync.WaitGroup{}
	for id := 0; id < args.parallel; id++ {
		waiter.Add(1)
		go getData(id, docChan, &waiter, args)
	}

	// Get documents that are all originals and assurred to be the true latest files
	allDocs := make(map[string]map[string]sproket.Doc)
	limit := 250
	for cur := 0; ; cur += limit {
		docs, remaining := args.search.SearchURLs(cur, limit)
		for _, doc := range docs {
			if !(args.softDataNode) {
				docChan <- doc
			} else {
				allDocs[doc.InstanceID] = make(map[string]sproket.Doc)
				allDocs[doc.InstanceID][doc.DataNode] = doc
			}
		}
		if remaining == 0 {
			break
		}
	}

	// Find replica options if desired
	if args.softDataNode {
		// Build list of potential alternative data nodes
		var validDataOptions []string
		for dataNodeMatch := range dataNodeMatches {
			validDataOptions = append(validDataOptions, dataNodeMatch)
		}
		// Restrict to this candidate data node only
		args.search.Fields["data_node"] = strings.Join(validDataOptions, " OR ")
		// These data nodes are replicas
		args.search.Fields["replica"] = "true"
		if args.verbose {
			fmt.Println(args.search)
		}

		// Find candidate docs and verify if the version is the true latest version using the instance_id key
		for cur := 0; ; cur += limit {
			docs, remaining := args.search.SearchURLs(cur, limit)
			for _, doc := range docs {
				_, in := allDocs[doc.InstanceID]
				if in {
					allDocs[doc.InstanceID][doc.DataNode] = doc
				}
			}
			if remaining == 0 {
				break
			}
		}

		jobsSubmitted := 0
		prefJobsSubmitted := 0
		for _, dataNodeMap := range allDocs {
			foundPreffered := false
			for _, prefferedDataNode := range args.search.DataNodePriority {
				for dataNode, doc := range dataNodeMap {
					if prefferedDataNode == dataNode {
						docChan <- doc
						foundPreffered = true
						jobsSubmitted++
						prefJobsSubmitted++
						break
					}
				}
				if foundPreffered {
					break
				}
			}
			if !(foundPreffered) {
				for _, doc := range dataNodeMap {
					docChan <- doc
					jobsSubmitted++
					break
				}
			}
		}
		if args.verbose {
			fmt.Printf("%d downloads submitted total\n", jobsSubmitted)
			fmt.Printf("%d preferred downloads submitted\n", prefJobsSubmitted)
		}
	}
	close(docChan)
	waiter.Wait()
}

func outputFields(args *config) {

	// Grab sample fields from a single search result
	keys := args.search.GetFields()
	if keys == nil {
		fmt.Println("no records match the search criteria, unable to determine fields")
		return
	}
	sort.Strings(keys)
	if args.verbose {
		fmt.Println(args.search)
	}
	for _, key := range keys {
		if !(strings.HasPrefix(key, "_")) {
			fmt.Println(key)
		}
	}
}

func outputDataNodes(args *config) {

	_, n := args.search.SearchURLs(0, 0)
	if n == 0 {
		fmt.Println("no records match search criteria")
		return
	}

	var dataNodeOutput []string

	// Ensure only unique files are output
	args.search.Fields["replica"] = "false"
	dataNodes := args.search.Facet("data_node")
	fmt.Println("excluding replication:")
	if args.verbose {
		fmt.Println(args.search)
	}
	if len(dataNodes) == 0 {
		fmt.Println("an original data node is required for download from any data nodes and no original data node was found")
		return
	}
	for dataNode := range dataNodes {
		dataNodeOutput = append(dataNodeOutput, dataNode)
	}
	sort.Strings(dataNodeOutput)
	// Output info
	for _, dataNode := range dataNodeOutput {
		fmt.Println(dataNode)
	}
	fmt.Println()

	// Ensure all files are counted
	args.search.Fields["replica"] = "*"

	// Get data node counts and total count
	dataNodes = args.search.Facet("data_node")
	dataNodeOutput = nil
	for dataNode := range dataNodes {
		dataNodeOutput = append(dataNodeOutput, dataNode)
	}
	sort.Strings(dataNodeOutput)
	// Output info
	fmt.Println("including replication:")
	if args.verbose {
		fmt.Println(args.search)
	}
	for _, dataNode := range dataNodeOutput {
		fmt.Println(dataNode)
	}
}

func outputValuesFor(args *config) {
	blacklistSubstrings := []string{"*"}
	for _, substring := range blacklistSubstrings {
		if strings.Contains(args.valuesFor, substring) {
			fmt.Printf("the values for field may not contain '%s'\n", substring)
			return
		}
	}
	blacklist := []string{"_timestamp", "timestamp", "id", "dataset_id", "master_id", "version", "citation_url", "data_specs_version", "datetime_start", "datetime_stop", "east_degrees", "west_degrees", "north_degrees", "geo", "height_bottom", "height_top", "instance_id", "number_of_aggregations", "number_of_files", "pid", "size", "south_degrees", "url", "title", "xlink", "_version_"}
	for _, field := range blacklist {
		if field == args.valuesFor {
			fmt.Printf("'%s' is not an allowed field to search for values for\n", args.valuesFor)
			return
		}
	}
	// Ensure only unique files are output
	args.search.Fields["replica"] = "false"
	if args.verbose {
		fmt.Println(args.search)
	}
	_, n := args.search.SearchURLs(0, 0)
	if n == 0 {
		fmt.Println("no records match search criteria")
		return
	}

	var values []string
	valueCounts := args.search.Facet(args.valuesFor)
	if len(valueCounts) == 0 {
		fmt.Printf("no values could be found for the provided field: '%s'\n", args.valuesFor)
		return
	}
	for value := range valueCounts {
		values = append(values, value)
	}
	sort.Strings(values)
	// Output info
	for _, value := range values {
		fmt.Println(value)
	}
}

func main() {

	var args config
	flag.StringVar(&args.conf, "config", "", "Path to config file")
	flag.StringVar(&args.outDir, "out.dir", ".", "Path to directory to put downloads in")
	flag.StringVar(&args.valuesFor, "values.for", "", "Display the available values for a given field, within the result set of the provided search criteria")
	flag.IntVar(&args.parallel, "p", 4, "Max number of concurrent downloads")
	flag.BoolVar(&args.noDownload, "no.download", false, "Flag to indicate no downloads should be performed")
	flag.BoolVar(&args.verbose, "verbose", false, "Flag to indicate output should be verbose")
	flag.BoolVar(&args.confirm, "y", false, "Flag to confirm larger downloads")
	flag.BoolVar(&args.noVerify, "no.verify", false, "Flag to skip checksum verification")
	flag.BoolVar(&args.fieldKeys, "field.keys", false, "Flag to output possible field keys. The outputted list may be incomplete for complicated reasons.")
	flag.BoolVar(&args.displayDataNodes, "data.nodes", false, "Flag to output data nodes that serve the files that match the criteria")
	flag.BoolVar(&args.count, "count", false, "Flag to only count number of files that would be attempted to be downloaded")
	flag.BoolVar(&args.version, "version", false, "Flag to output the version and exit")
	flag.BoolVar(&args.urlsOnly, "urls.only", false, "Flag to only output to stdout the HTTP URLs that would be used")
	flag.BoolVar(&args.unsafe, "unsafe", false, "Removes the hard set requirement of the retracted field being false and latest being true. The user is then free to specify these fields themselves in the search config, but are not required to.")
	flag.Parse()
	if args.version {
		fmt.Println(VERSION)
		return
	}
	// Everything beyond this point requires an initialized Search object
	if args.conf == "" {
		fmt.Println("-config is required, use -h for help")
		return
	}
	err := args.Init()
	if err != nil {
		fmt.Println(err)
		return
	}
	if args.displayDataNodes {
		outputDataNodes(&args)
	} else if args.valuesFor != "" {
		outputValuesFor(&args)
	} else if args.fieldKeys {
		outputFields(&args)
	} else if len(args.search.Fields) > 0 {
		getBySearch(&args)
	} else {
		flag.Usage()
	}
}
