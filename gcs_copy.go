package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/piplcom/gcs_copy/conf"
	ppaths "github.com/piplcom/gcs_copy/paths"
	"github.com/piplcom/gcs_copy/transfer"
	"golang.org/x/exp/slog"
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
	api    = flag.Bool("api", false, "if true open port and get config via api")
	port   = flag.Int("port", 8082, "port to listen on")
	bindip = flag.String("bindip", "0.0.0.0", "ip to bind to")
	fcred  = flag.String("cred", "", "credential path")
	fin    = flag.String("in", "", "input dir path, starting with gs:// for bucket or just / for dir")
	fout   = flag.String("out", "", "output dir path, starting with gs:// for bucket or just / for dir")
	fconc  = flag.Int("conc", 64, "upload cuncurrency")
	fcheck = flag.Bool("check", false, "check only")
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)
var logger *slog.Logger

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Println(err)
	}
	ip, err := getIP()
	if err != nil {
		log.Println(err)
	}
	textHandler := slog.NewTextHandler(os.Stdout).WithAttrs([]slog.Attr{
		slog.String("app", "gcs_copy"),
		slog.String("hostname", hostname),
		slog.String("ip", ip)})
		
	logger = slog.New(textHandler)
	slog.SetDefault(logger)

	log.Printf("my app %s, commit %s, built at %s\n", version, commit, date)

	flag.Parse()

	if *api {
		http.HandleFunc("/state", handleGetStatus)
		http.HandleFunc("/run", handleRunCopy)
		http.HandleFunc("/size", handleSize)
		log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", *bindip, *port), nil))
	} else {
		var Args = conf.Args{
			Conc:  *fconc,
			In:    *fin,
			Out:   *fout,
			Cred:  *fcred,
			Check: *fcheck,
		}
		runCopy(Args)
	}

}

func runCopy(args conf.Args) {

	log.Printf("---------------------------------------------\n")
	log.Printf("credential: %s\t\t\n", args.Cred)
	log.Printf("input:      %s\t\t\n", args.In)
	log.Printf("output:     %s\t\t\n", args.Out)
	log.Printf("concurrent workers:     %d\t\t\n", args.Conc)
	log.Printf("---------------------------------------------\n\n")
	if args.Check {
		log.Println("DRY RUN! (check option is checked)")
	}

	var ItemsToTransfer ppaths.Items
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
	ItemsToTransferChan := ppaths.NewItemsToTransferChan()
	var func2run func(args conf.Args, wg *sync.WaitGroup, c *chan ppaths.Item)

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
	log.Println("we will scan now local dir and the bucket, might take time depending on number of files")
	log.Println("even half an hour for millions of files")
	log.Println("for small ammount of files whould take few seconds")

	go ppaths.PWalkDir(localRoot, &ppaths.AllFiles, &walkWg)
	go ppaths.WalkBucket(bucketRoot, &ppaths.AllObjects, &walkWg, *fcred)
	walkWg.Wait()
	ppaths.FillItemsToTransfer(*itemObjects["in"], *itemObjects["out"], &ItemsToTransfer)
	// ppaths.FillItemsToTransfer(ppaths.AllFiles, ppaths.AllObjects)
	go ppaths.Slice2Chan(ItemsToTransfer, *ItemsToTransferChan)
	i, s := ppaths.ItemsSum(ItemsToTransfer)

	if len(ItemsToTransfer.List) > 0 {
		log.Printf("number of files to transfer: %v\ntotal size is: %v Bytes (%.2f) GB\n", i, s, float64(s)/1024/1024/1024)
	} else {
		log.Println("all files are the same size, there is nothing to transfer")
	}

	if !args.Check {
		log.Println("started transfer at: ", time.Now())
		transfer.Transfer(args, ItemsToTransferChan, func2run)
		log.Println("finished transfer at: ", time.Now())
		state = "done"
	}

}

func getIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}
