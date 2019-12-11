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
	"os"
	"sort"
	"sproket"
	"strings"
	"sync"
)

type config struct {
	conf             string
	outDir           string
	parallel         int
	noDownload       bool
	verbose          bool
	confirm          bool
	count            bool
	noVerify         bool
	version          bool
	fieldKeys        bool
	displayDataNodes bool
	softDataNode     bool
	search           sproket.Search
}

func (args *config) Init() error {

	if args.conf != "" {
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
		args.search.Fields["retracted"] = "false"
		args.search.Fields["latest"] = "true"

		args.softDataNode = (len(args.search.DataNodePriority) != 0)
	}
	if _, err := os.Stat(args.outDir); os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist", args.outDir)
	}
	return nil
}

func verify(dest string, remoteSum string, remoteSumType string) error {

	if remoteSum == "" || remoteSumType == "" {
		return fmt.Errorf("could not retrieve checksum for %s", dest)
	}
	f, err := os.Open(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	var hash hash.Hash
	switch remoteSumType {
	case "MD5":
		hash = md5.New()
	case "SHA256":
		hash = sha256.New()
	default:
		return fmt.Errorf("unrecognized checksum_type: %s", remoteSumType)
	}

	if _, err := io.Copy(hash, f); err != nil {
		return err
	}
	res := fmt.Sprintf("%x", hash.Sum(nil))
	if res != remoteSum {
		fmt.Printf("%s != %s\n", res, remoteSum)
		return fmt.Errorf("checksum verification failure for %s", dest)
	}
	return nil
}

func getData(id int, inDocs <-chan sproket.Doc, waiter *sync.WaitGroup, args *config) {
	defer waiter.Done()
	for doc := range inDocs {
		if args.verbose {
			fmt.Printf("%d: download %s\n", id, doc.HTTPURL)
		}
		if args.noDownload {
			if args.verbose {
				fmt.Printf("%d: no download\n", id)
			}
		} else {

			dest := fmt.Sprintf("%s/%s", args.outDir, doc.InstanceID)

			// Check if present and correct
			if _, err := os.Stat(dest); err == nil {
				err := verify(dest, doc.GetSum(), doc.GetSumType())
				if err != nil {
					fmt.Println(err)
				} else {
					if args.verbose {
						fmt.Printf("%d: %s already present and verified, no download\n", id, dest)
					}
					// Go to next download if everything checks out
					continue
				}
			}

			// Perform download
			err := sproket.Get(doc.HTTPURL, dest)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// Verify checksum, if available and desired
			if !(args.noVerify) {
				err := verify(dest, doc.GetSum(), doc.GetSumType())
				if err != nil {
					fmt.Println(err)
				} else if args.verbose {
					fmt.Printf("%d: verified %s\n", id, dest)
				}
			}
		}
	}
}

func getBySearch(search sproket.Search, args *config) {

	// Check if the soft data node list will even matter
	dataNodeMatches := make(map[string]bool)
	if args.softDataNode {
		// Check for any matching replica data nodes in data node priority list
		search.Fields["replica"] = "true"
		dataNodes := sproket.DataNodes(&search)
		for dataNode := range dataNodes {
			for _, preferedDataNode := range search.DataNodePriority {
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
	}

	// Count original files, only files with replica: false entries present in the index will be downloaded
	search.Fields["replica"] = "false"
	if args.verbose {
		fmt.Println(search)
	}
	_, n := sproket.SearchURLs(&search, 0, 0)
	fmt.Printf("found %d files for download\n", n)
	if args.count || n == 0 {
		return
	}
	if !(args.confirm) && n > 100 {
		fmt.Println("too many files (>100): confirm larger download by specifying the -y option or refine search criteria")
		return
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
	cur := 0
	for {
		docs, remaining := sproket.SearchURLs(&search, cur, limit)
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
		cur += limit
	}

	// Find replica options if desired
	if args.softDataNode {
		cur = 0
		search.Fields["replica"] = "true"

		var validDataOptions []string
		for dataNodeMatch := range dataNodeMatches {
			validDataOptions = append(validDataOptions, dataNodeMatch)
		}
		search.Fields["data_node"] = strings.Join(validDataOptions, " OR ")
		if args.verbose {
			fmt.Println(search)
		}
		for {
			docs, remaining := sproket.SearchURLs(&search, cur, limit)
			for _, doc := range docs {
				_, in := allDocs[doc.InstanceID]
				if in {
					allDocs[doc.InstanceID][doc.DataNode] = doc
				}
			}
			if remaining == 0 {
				break
			}
			cur += limit
		}

		jobsSubmitted := 0
		prefJobsSubmitted := 0
		for _, dataNodeMap := range allDocs {
			foundPreffered := false
			for _, prefferedDataNode := range search.DataNodePriority {
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
		fmt.Printf("%d downloads submitted total\n", jobsSubmitted)
		fmt.Printf("%d preferred downloads submitted\n", prefJobsSubmitted)
	}
	close(docChan)
	waiter.Wait()
}

func outputFields(args *config) {

	// Grab sample fields from a single search result
	keys := sproket.SearchFields(&args.search)
	sort.Strings(keys)
	fmt.Println("criteria: ")
	fmt.Println(args.search)
	fmt.Println("field keys: ")
	for _, key := range keys {
		if !(strings.HasPrefix(key, "_")) {
			fmt.Printf("  %s\n", key)
		}
	}
	fmt.Println()
}

func outputDataNodes(args *config) {

	var dataNodeOutput []string

	// Ensure only unique files are output
	args.search.Fields["replica"] = "false"
	dataNodes := sproket.DataNodes(&args.search)
	fmt.Println("excluding replication:")
	fmt.Println(args.search)
	if len(dataNodes) == 0 {
		fmt.Println("an original data node is required for download from any data nodes and no original data node was found")
		return
	}
	dataNodeOutput = nil
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
	dataNodes = sproket.DataNodes(&args.search)

	for dataNode := range dataNodes {
		dataNodeOutput = append(dataNodeOutput, dataNode)
	}
	sort.Strings(dataNodeOutput)
	// Output info
	fmt.Println("including replication:")
	fmt.Println(args.search)
	for _, dataNode := range dataNodeOutput {
		fmt.Println(dataNode)
	}
}

func main() {

	var args config
	flag.StringVar(&args.conf, "config", "", "Path to config file")
	flag.StringVar(&args.outDir, "out.dir", "./", "Path to directory to put downloads in")
	flag.IntVar(&args.parallel, "p", 4, "Max number of concurrent downloads")
	flag.BoolVar(&args.noDownload, "no.download", false, "Flag to indicate no downloads should be performed")
	flag.BoolVar(&args.verbose, "verbose", false, "Flag to indicate output should be verbose")
	flag.BoolVar(&args.confirm, "y", false, "Flag to confirm larger downloads")
	flag.BoolVar(&args.noVerify, "no.verify", false, "Flag to skip checksum verification")
	flag.BoolVar(&args.fieldKeys, "field.keys", false, "Flag to output possible field keys. The outputted list may be incomplete for complicated reasons.")
	flag.BoolVar(&args.displayDataNodes, "data.nodes", false, "Flag to output data nodes that serve the files that match the criteria")
	flag.BoolVar(&args.count, "count", false, "Flag to only count number of files that would be attempted to be downloaded")
	flag.BoolVar(&args.version, "version", false, "Flag to output the version and exit")
	flag.Parse()
	if args.version {
		fmt.Printf("v0.2.3\n")
		return
	}
	err := args.Init()
	if err != nil {
		fmt.Println(err)
		return
	}
	if args.displayDataNodes {
		outputDataNodes(&args)
	} else if args.fieldKeys {
		outputFields(&args)
	} else if len(args.search.Fields) > 0 {
		getBySearch(args.search, &args)
	} else {
		flag.Usage()
	}
}
