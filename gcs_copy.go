package main

import (
	"flag"
	"fmt"

	// "log"
	// "os"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/yosefy/gcp_copy/conf"
	ppaths "github.com/yosefy/gcp_copy/paths"
	"github.com/yosefy/gcp_copy/transfer"
)

func main() {

	// log := log.New(os.Stdout, "MAIN : ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	// var err error
	Formatter := new(log.TextFormatter)
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	// config
	var (
		fcred  = flag.String("cred", "", "credential path")
		fin    = flag.String("in", "", "input dir path, starting with gs:// for bucket or just / for dir")
		fout   = flag.String("out", "", "output dir path, starting with gs:// for bucket or just / for dir")
		fconc  = flag.Int("conc", 64, "upload cuncurrency")
		fcheck = flag.Bool("check", false, "check only")

		localRoot, bucketRoot string
		itemObjects = make(map[string]*ppaths.Items)
	)
	flag.Parse()

	cred := *fcred
	in := strings.TrimSuffix(*fin, "/")
	out := strings.TrimSuffix(*fout, "/")
	conc := *fconc
	check := *fcheck



	var args = conf.Args{
		Conc: conc,
		In:   in,
		Out:  out,
		Cred: cred,
	}

	fmt.Println(args, check)
	direction, err := ppaths.Direction(*fin, *fout)
	if err != nil {
		log.Fatal("wrong parameters type, should start with '/' or 'gs://'")
	}

	fmt.Printf("---------------------------------------------\n")
	fmt.Printf("credential: %s\t\t\n", cred)
	fmt.Printf("input:      %s\t\t\n", in)
	fmt.Printf("output:     %s\t\t\n", out)
	fmt.Printf("concurrent workers:     %d\t\t\n", conc)
	fmt.Printf("direction:     %s\t\t\n", direction)
	fmt.Printf("---------------------------------------------\n\n")

	var walkWg sync.WaitGroup
	walkWg.Add(2)
	var func2run func(args conf.Args, wg *sync.WaitGroup)

	switch {
	case direction == "local2bucket":
		localRoot, bucketRoot = in, out
		itemObjects["in"] = &ppaths.AllFiles
		itemObjects["out"] = &ppaths.AllObjects
		func2run = transfer.CreateUploadRoutines
	case direction == "bucket2local":
		bucketRoot, localRoot = in, out
		itemObjects["in"] = &ppaths.AllObjects
		itemObjects["out"] = &ppaths.AllFiles
		func2run = transfer.CreateDownloadRoutines
	}

	go ppaths.PWalkDir(localRoot, &ppaths.AllFiles, &walkWg)
	go ppaths.WalkBucket(bucketRoot, &ppaths.AllObjects, &walkWg, cred)
	walkWg.Wait()
	ppaths.FillItemsToTransfer(*itemObjects["in"], *itemObjects["out"])
	// ppaths.FillItemsToTransfer(ppaths.AllFiles, ppaths.AllObjects)

	go ppaths.Slice2Chan(ppaths.ItemsToTransfer, ppaths.ItemsToTransferChan)
	i, s := ppaths.ItemsSum(ppaths.ItemsToTransfer)
	if len(ppaths.ItemsToTransfer.List) > 0 {
		fmt.Printf("number of files to transfer: %v\ntotal size is: %v Bytes (%.2f) GB\n", i, s, float64(s)/1024/1024/1024)
		} else {
			fmt.Println("all files are the same size, there is nothing to transfer")
		}
		
		if !check {
			transfer.Transfer(args, func2run)
		}


}
