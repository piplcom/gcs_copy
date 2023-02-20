package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	// "log"
	// "os"
	"strings"
	"sync"

	"github.com/piplcom/gcs_copy/conf"
	ppaths "github.com/piplcom/gcs_copy/paths"
	"github.com/piplcom/gcs_copy/transfer"
	log "github.com/sirupsen/logrus"
)

var (
	bucketRoot, localRoot string
	in, out               string
	check                 bool
	conc                  int
	cred                  string
	state                 string
)

var (
	api = flag.Bool("api", false, "if true open port and get config via api")
	port = flag.Int("port", 8082, "port to listen on")
	bindip = flag.String("bindip", "0.0.0.0", "ip to bind to")
	fcred   = flag.String("cred", "", "credential path")
	fin     = flag.String("in", "", "input dir path, starting with gs:// for bucket or just / for dir")
	fout    = flag.String("out", "", "output dir path, starting with gs:// for bucket or just / for dir")
	fconc   = flag.Int("conc", 64, "upload cuncurrency")
	fcheck  = flag.Bool("check", false, "check only")
	// log_level = flag.String("check", "INFO", "log level")
)


var (
    version = "dev"
    commit  = "none"
    date    = "unknown"
)

func main() {
	fmt.Println("test")
	fmt.Println("version: ", Version)
	fmt.Printf("my app %s, commit %s, built at %s", version, commit, date)
	// log := log.New(os.Stdout, "MAIN : ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	// var err error
	// Formatter := new(log.TextFormatter)
	// Formatter.TimestampFormat = "02-01-2006 15:04:05"
	// Formatter.FullTimestamp = true
	// log.SetFormatter(Formatter)

	// config

	flag.Parse()


	
	if *api {
		http.HandleFunc("/state", handleGetStatus)
		http.HandleFunc("/run", handleRunCopy)
		http.HandleFunc("/size", handleSize)
		log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", *bindip, *port), nil))
		// log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%s", bindIP, bindPort), nil))
	} else {
		cred := *fcred
		in := strings.TrimSuffix(*fin, "/")
		out := strings.TrimSuffix(*fout, "/")
		conc := *fconc
		check = *fcheck
	
		// fmt.Println(args, check)
		// direction, err := ppaths.Direction(*fin, *fout)
		// log.Println()
		// if err != nil {
		// 	log.Fatal("wrong parameters type, should start with '/' or 'gs://'")
		// }


		var Args = conf.Args{
			Conc: conc,
			In:   in,
			Out:  out,
			Cred: cred,
			Check: check,

		}
		runCopy(Args)
	}

}

func runCopy(args conf.Args) {


	fmt.Printf("---------------------------------------------\n")
	fmt.Printf("credential: %s\t\t\n", args.Cred)
	fmt.Printf("input:      %s\t\t\n", args.In)
	fmt.Printf("output:     %s\t\t\n", args.Out)
	fmt.Printf("concurrent workers:     %d\t\t\n", args.Conc)
	fmt.Printf("---------------------------------------------\n\n")
	if args.Check {
		fmt.Println("DRY RUN! (check option is checked)")
	}

	var ItemsToTransfer     ppaths.Items
	var itemObjects = make(map[string]*ppaths.Items)
	ppaths.AllFiles.List = nil
	ppaths.AllObjects.List = nil
	direction, err := ppaths.Direction(args.In, args.Out)
	if err != nil {
		log.Println("outer in", args.In)
		log.Println("outer out", args.Out)
		log.Fatal("wrong parameters type, should start with '/' or 'gs://'")
	}

	var walkWg sync.WaitGroup
	walkWg.Add(2)
	ItemsToTransferChan :=  ppaths.NewItemsToTransferChan()
	var func2run func(args conf.Args, wg *sync.WaitGroup, c *chan ppaths.Item )

	switch {
	case direction == "local2bucket":
		localRoot, bucketRoot = args.In, args.Out

		itemObjects["in"] = &ppaths.AllFiles
		itemObjects["out"] = &ppaths.AllObjects
		func2run = transfer.CreateUploadRoutines
	case direction == "bucket2local":
		bucketRoot, localRoot = args.In, args.Out
		itemObjects["in"] = &ppaths.AllObjects
		itemObjects["out"] = &ppaths.AllFiles
		func2run = transfer.CreateDownloadRoutines
	}

	log.Println("started at: ", time.Now())
	fmt.Println("we will scan now local dir and the bucket, might take time depending on number of files")
	fmt.Println("even half an hour for millions of files")
	fmt.Println("for small ammount of files whould take few seconds")

	go ppaths.PWalkDir(localRoot, &ppaths.AllFiles, &walkWg)
	go ppaths.WalkBucket(bucketRoot, &ppaths.AllObjects, &walkWg, *fcred)
	walkWg.Wait()
	ppaths.FillItemsToTransfer(*itemObjects["in"], *itemObjects["out"], &ItemsToTransfer)
	// ppaths.FillItemsToTransfer(ppaths.AllFiles, ppaths.AllObjects)
	go ppaths.Slice2Chan(ItemsToTransfer,*ItemsToTransferChan)
	i, s := ppaths.ItemsSum(ItemsToTransfer)

	if len(ItemsToTransfer.List) > 0 {
		fmt.Printf("number of files to transfer: %v\ntotal size is: %v Bytes (%.2f) GB\n", i, s, float64(s)/1024/1024/1024)
	} else {
		fmt.Println("all files are the same size, there is nothing to transfer")
	}

	if !args.Check {
		fmt.Println("started transfer at: ", time.Now())
		transfer.Transfer(args, ItemsToTransferChan, func2run)
		fmt.Println("finished transfer at: ", time.Now())
		state = "done"
	}

}
