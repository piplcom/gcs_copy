package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"log/slog"
)

type Args struct {
	Conc  int
	In    string
	Out   string
	Cred  string
	Check bool
}

var (
	bucketRoot, localRoot string
	in, out               string
	check                 bool
	conc                  int
	cred                  string
)


type State struct {
	ItemsNumberCurrent int
	ItemsSizeCurrent   int64
	State              string
	Error              string
}

var Pstate State

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
	textHandler := slog.NewTextHandler(os.Stdout, nil).WithAttrs([]slog.Attr{
		slog.String("app", "gcs_copy"),
		slog.String("hostname", hostname),
		slog.String("ip", ip)})

	logger = slog.New(textHandler)
	slog.SetDefault(logger)
	flag.Parse()

	log.Printf("my app %s, commit %s, built at %s\n", version, commit, date)




	if *api {
		http.HandleFunc("/state", handleGetStatus)
		http.HandleFunc("/run", handleRunCopy)
		http.HandleFunc("/size", handleSize)

		server := &http.Server{
			Addr: fmt.Sprintf("%s:%d", *bindip, *port),
		}

		go func() {
			if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("HTTP server error: %v", err)
			}
			log.Println("Stopped serving new connections.")
		}()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownRelease()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("HTTP shutdown error: %v", err)
		}
		log.Println("Graceful shutdown complete.")

	} else {
		var Args = Args{
			Conc:  *fconc,
			In:    *fin,
			Out:   *fout,
			Cred:  *fcred,
			Check: *fcheck,
		}
		runCopy(Args)
	}

}

func runCopy(args Args) {

	// go func() {
	// 	diskTicker := time.NewTicker(time.Minute * 1)
	// 	for ; true; <-diskTicker.C {
	// 		err := SetErrorStateIfNoSpace()
	// 		if err != nil{
	// 			log.Println(err)
	// 		}
	// 	}
	// }()

	log.Printf("starting gcs_copy with credential: %s, input: %s, output: %s, conc: %d\n", args.Cred, args.In, args.Out, args.Conc)

	if args.Check {
		log.Println("DRY RUN! (check option is checked)")
	}

	var ItemsToTransfer Items
	var itemObjects = make(map[string]*Items)
	AllFiles.List = nil
	AllObjects.List = nil
	direction, err := Direction(args.In, args.Out)
	if err != nil {
		log.Println("outer in", args.In)
		log.Println("outer out", args.Out)
		log.Fatal("wrong parameters type, should start with '/' or 'gs://'")
	}

	var walkWg sync.WaitGroup
	walkWg.Add(2)
	ItemsToTransferChan := NewItemsToTransferChan()
	var func2run func(args Args, wg *sync.WaitGroup, c *chan Item)

	switch {
	case direction == "local2bucket":
		localRoot, bucketRoot = args.In, args.Out

		itemObjects["in"] = &AllFiles
		itemObjects["out"] = &AllObjects
		func2run = CreateUploadRoutines
	case direction == "bucket2local":
		bucketRoot, localRoot = args.In, args.Out
		itemObjects["in"] = &AllObjects
		itemObjects["out"] = &AllFiles
		func2run = CreateDownloadRoutines
	}

	log.Println("gcs_copy started at: ", time.Now())

	go PWalkDir(localRoot, &AllFiles, &walkWg)
	go WalkBucket(direction, bucketRoot, &AllObjects, &walkWg, *fcred)
	walkWg.Wait()
	FillItemsToTransfer(*itemObjects["in"], *itemObjects["out"], &ItemsToTransfer)
	// ppaths.FillItemsToTransfer(ppaths.AllFiles, ppaths.AllObjects)
	go Slice2Chan(ItemsToTransfer, *ItemsToTransferChan)
	i, s := ItemsSum(ItemsToTransfer)

	if len(ItemsToTransfer.List) > 0 {
		log.Printf("number of files to transfer: %v\ntotal size is: %v Bytes (%.2f) GB\n", i, s, float64(s)/1024/1024/1024)
	} else {
		log.Println("all files are the same size, there is nothing to transfer")
	}

	if !args.Check {
		log.Println("started transfer at: ", time.Now())
		Transfer(args, ItemsToTransferChan, func2run)
		log.Println("finished transfer at: ", time.Now())
		Pstate.State = "done"
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
