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
	"log"
	"os"
	"sort"
	"sproket"
	"strings"
	"sync"
)

type config struct {
	conf       string
	outDir     string
	parallel   int
	datasetIds bool
	fileIds    bool
	noDownload bool
	verbose    bool
	confirm    bool
	count      bool
	noVerify   bool
	version    bool
	fieldKeys  bool
	searchAPI  string
	criteria   []sproket.Criteria
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
		var downloads sproket.Downloads
		json.Unmarshal(fileBytes, &downloads)
		if downloads.SAPI == "" {
			return fmt.Errorf("search_api is required parameter in config file")
		}
		args.searchAPI = downloads.SAPI
		args.criteria = downloads.Reqs
	}
	if _, err := os.Stat(args.outDir); os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist", args.outDir)
	}
	if mutuallyExclude(args.fileIds, args.datasetIds) {
		return errors.New("incompatible arguments, -file.ids and -dataset.ids are mutually exclusive")
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

			// Perform download
			sproket.Get(doc.HTTPURL, dest)

			// Verify checksum, if available and desired
			if doc.GetSum() != "" && !(args.noVerify) {
				f, err := os.Open(dest)
				if err != nil {
					log.Fatal(err)
				}
				hash := sha256.New()
				if _, err := io.Copy(hash, f); err != nil {
					log.Fatal(err)
				}
				res := fmt.Sprintf("%x", hash.Sum(nil))
				if res != doc.GetSum() {
					fmt.Printf("%d: checksum failure %s\n", id, dest)
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
	for i := 0; i < len(criteria); i++ {
		if criteria[i].Disabled {
			continue
		}
		_, n := sproket.SearchURLs(&criteria[i], args.searchAPI, criteria[i].Start, 0)
		if args.verbose {
			fmt.Println(criteria[i])
			fmt.Printf("found %d files to download\n", n)
		}
		totalFiles += n
	}
	fmt.Printf("total files: %d\n", totalFiles)
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
	started := 0
	for i := 0; i < len(criteria); i++ {
		if criteria[i].Disabled {
			continue
		}
		cur := criteria[i].Start
		for {
			docs, remaining := sproket.SearchURLs(&criteria[i], args.searchAPI, cur, limit)
			for _, doc := range docs {
				docChan <- doc
			}
			started += len(docs)
			if args.verbose {
				fmt.Printf("downloads started: %d of %d\n", started, totalFiles)
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
	for i := 0; i < len(args.criteria); i++ {
		if args.criteria[i].Disabled {
			continue
		}
		keys := sproket.SearchFields(&args.criteria[i], args.searchAPI)
		sort.Strings(keys)
		fmt.Println("criteria: ")
		fmt.Println(args.criteria[i])
		fmt.Println("field keys: ")
		for _, key := range keys {
			if !(strings.HasPrefix(key, "_")) {
				fmt.Printf("  %s\n", key)
			}
		}
		fmt.Println()
	}
}

func loadStdin() []string {
	scanner := bufio.NewScanner(os.Stdin)
	var strs []string
	for scanner.Scan() {
		strs = append(strs, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return strs
}

func main() {

	var args config
	flag.StringVar(&args.conf, "config", "", "Path to config file")
	flag.StringVar(&args.outDir, "out.dir", "./", "Path to directory to put downloads in")
	flag.IntVar(&args.parallel, "p", 4, "Max number of conncurrent downloads")
	flag.BoolVar(&args.datasetIds, "dataset.ids", false, "Flag to indicate dataset ids are being provided on standard in")
	flag.BoolVar(&args.fileIds, "file.ids", false, "Flag to indicate file ids are being provided on standard in")
	flag.BoolVar(&args.noDownload, "no.download", false, "Flag to indicate no downloads should be performed")
	flag.BoolVar(&args.verbose, "verbose", false, "Flag to indicate output should be verbose")
	flag.BoolVar(&args.confirm, "y", false, "Flag to confirm larger downloads")
	flag.BoolVar(&args.noVerify, "no.verify", false, "Flag to skip sha256 verification")
	flag.BoolVar(&args.fieldKeys, "field.keys", false, "Flag to output possible field keys. The outputted list may be incomplete for complicated reasons.")
	flag.BoolVar(&args.count, "count", false, "Flag to only count number of files that would be attempted to be downloaded")
	flag.BoolVar(&args.version, "version", false, "Flag to output the version and exit")
	flag.Parse()
	if args.version {
		fmt.Printf("v0.0.1\n")
		return
	}
	err := args.Init()
	if err != nil {
		fmt.Println(err)
		return
	}
	if args.fieldKeys {
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
