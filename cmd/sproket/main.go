package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	datasetIds       bool
	fileIds          bool
	noDownload       bool
	verbose          bool
	confirm          bool
	count            bool
	noVerify         bool
	version          bool
	fieldKeys        bool
	displayDataNodes bool
	searchAPI        string
	criteria         []sproket.Criteria
}

func mutuallyExclude(opts ...bool) bool {
	sum := 0
	for _, opt := range opts {
		if opt {
			sum++
		}
	}
	return sum > 1
}

func (args *config) Init() error {

	if args.conf != "" {
		fileBytes, err := ioutil.ReadFile(args.conf)
		if err != nil {
			return fmt.Errorf("%s not found", args.conf)
		}
		if !(json.Valid(fileBytes)) {
			return fmt.Errorf("%s does not contain valid JSON", args.conf)
		}

		var downloads sproket.Downloads
		json.Unmarshal(fileBytes, &downloads)
		if downloads.SAPI == "" {
			return fmt.Errorf("search_api is required parameter in config file")
		}
		args.searchAPI = downloads.SAPI
		args.criteria = downloads.Reqs
		for _, c := range args.criteria {
			c.Fields["replica"] = "*"
			c.Fields["retracted"] = "false"
			c.Fields["latest"] = "true"
		}
	}
	if _, err := os.Stat(args.outDir); os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist", args.outDir)
	}
	if mutuallyExclude(args.fileIds, args.datasetIds) {
		return errors.New("incompatible arguments, -file.ids and -dataset.ids are mutually exclusive")
	}
	return nil
}

func verify(dest string, sha256sum string) error {

	if sha256sum == "" {
		return fmt.Errorf("could not retrieve checksum for %s", dest)
	}
	f, err := os.Open(dest)
	if err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return err
	}
	res := fmt.Sprintf("%x", hash.Sum(nil))
	if res != sha256sum {
		return errors.New("sha256 verification failure")
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

			dest := fmt.Sprintf("%s/%s", args.outDir, doc.ID)

			// Check if present and correct
			if _, err := os.Stat(dest); err == nil {
				err := verify(dest, doc.GetSum())
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
			sproket.Get(doc.HTTPURL, dest)

			// Verify checksum, if available and desired
			if !(args.noVerify) {
				err := verify(dest, doc.GetSum())
				if err != nil {
					fmt.Println(err)
				} else if args.verbose {
					fmt.Printf("%d: verified %s\n", id, dest)
				}
			}
		}
	}
}

func getBySearch(criteria []sproket.Criteria, args *config) {

	// Count files to be downloaded
	totalFiles := 0
	for _, c := range criteria {
		if c.Disabled {
			continue
		}
		c.Fields["replica"] = "false"
		_, n := sproket.SearchURLs(&c, args.searchAPI, 0, 0)
		if args.verbose {
			fmt.Println(c)
			fmt.Printf("found %d unique files to download\n", n)
		}
		totalFiles += n
	}
	fmt.Printf("total unique files: %d\n", totalFiles)
	if args.count {
		return
	}
	if !(args.confirm) && totalFiles > 100 {
		fmt.Println("too many files (>100): confirm download by specifying the -y option or refine search criteria")
		return
	}

	// Set up concurrent workers
	docChan := make(chan sproket.Doc)
	waiter := sync.WaitGroup{}
	for id := 0; id < args.parallel; id++ {
		waiter.Add(1)
		go getData(id, docChan, &waiter, args)
	}

	// Begin grabbing sets of files to download
	limit := 50
	for _, c := range criteria {
		if c.Disabled {
			continue
		}
		cur := 0
		for {
			docs, remaining := sproket.SearchURLs(&c, args.searchAPI, cur, limit)
			for _, doc := range docs {
				docChan <- doc
			}
			if remaining == 0 {
				break
			}
			cur += limit
		}
	}
	close(docChan)
	waiter.Wait()
}

func getByIDs(ids []string, args *config) {
	var idField string
	if args.fileIds {
		idField = "id"
	}
	if args.datasetIds {
		idField = "dataset_id"
	}
	var criteria []sproket.Criteria
	for _, id := range ids {
		f := map[string]string{
			idField: id,
		}
		c := sproket.Criteria{
			Fields: f,
		}
		criteria = append(criteria, c)
	}
	getBySearch(criteria, args)
}

func outputFields(args *config) {
	for _, c := range args.criteria {
		if c.Disabled {
			continue
		}
		keys := sproket.SearchFields(&c, args.searchAPI)
		sort.Strings(keys)
		fmt.Println("criteria: ")
		fmt.Println(c)
		fmt.Println("field keys: ")
		for _, key := range keys {
			if !(strings.HasPrefix(key, "_")) {
				fmt.Printf("  %s\n", key)
			}
		}
		fmt.Println()
	}
}

func outputDataNodes(args *config) {
	for _, c := range args.criteria {
		if c.Disabled {
			continue
		}
		c.Fields["replica"] = "false"
		dataNodes := sproket.DataNodes(&c, args.searchAPI)
		_, n := sproket.SearchURLs(&c, args.searchAPI, 0, 0)
		fmt.Println(&c)
		for dataNode, count := range dataNodes {
			fmt.Printf("%s has %d of the %d unique files\n", dataNode, count, n)
		}
	}
}

func loadStdin() []string {
	scanner := bufio.NewScanner(os.Stdin)
	var strs []string
	for scanner.Scan() {
		strs = append(strs, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return strs
}

func main() {

	var args config
	flag.StringVar(&args.conf, "config", "", "Path to config file")
	flag.StringVar(&args.outDir, "out.dir", "./", "Path to directory to put downloads in")
	flag.IntVar(&args.parallel, "p", 4, "Max number of concurrent downloads")
	flag.BoolVar(&args.datasetIds, "dataset.ids", false, "Flag to indicate dataset ids are being provided on standard in")
	flag.BoolVar(&args.fileIds, "file.ids", false, "Flag to indicate file ids are being provided on standard in")
	flag.BoolVar(&args.noDownload, "no.download", false, "Flag to indicate no downloads should be performed")
	flag.BoolVar(&args.verbose, "verbose", false, "Flag to indicate output should be verbose")
	flag.BoolVar(&args.confirm, "y", false, "Flag to confirm larger downloads")
	flag.BoolVar(&args.noVerify, "no.verify", false, "Flag to skip sha256 verification")
	flag.BoolVar(&args.fieldKeys, "field.keys", false, "Flag to output possible field keys. The outputted list may be incomplete for complicated reasons.")
	flag.BoolVar(&args.displayDataNodes, "data.nodes", false, "Flag to output data nodes that serve the files that match each criteria")
	flag.BoolVar(&args.count, "count", false, "Flag to only count number of files that would be attempted to be downloaded")
	flag.BoolVar(&args.version, "version", false, "Flag to output the version and exit")
	flag.Parse()
	if args.version {
		fmt.Printf("v0.0.2\n")
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
	} else if args.fileIds || args.datasetIds {
		ids := loadStdin()
		getByIDs(ids, &args)
	} else if len(args.criteria) > 0 {
		getBySearch(args.criteria, &args)
	} else {
		flag.Usage()
	}
}
