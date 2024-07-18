package main

import (
	"context"
	"errors"
	"fmt"

	"log"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	// "strconv"
	"strings"

	"io/fs"
	"path/filepath"

	"cloud.google.com/go/storage"
	"golang.org/x/sys/unix"

	// log "github.com/sirupsen/logrus"
	// "golang.org/x/exp/slog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type ItemsToTransferChan chan Item

var (
	AllFiles   Items
	AllObjects Items
	// ItemsToTransfer     Items
	ItemsNumberCurrent int
	ItemsSizeCurrent   int64
)

func NewItemsToTransferChan() *chan Item {
	var ItemsToTransferChan = make(chan Item)
	return &ItemsToTransferChan
}

type Item struct {
	Path string
	Size int64
}

type Items struct {
	List []Item
}

type Disk struct {
	Mount     string
	FreeBytes uint64
}

func IsBucket(path string) bool {
	return strings.HasPrefix(path, "gs://")
}

func ExtrDirNameFromObj(s string) string {
	mb := regexp.MustCompile("^(.*/)[^/]*$")
	return mb.ReplaceAllString(s, "$1")
}

func ExtrBucketNameFromPath(path string) string {
	mb := regexp.MustCompile("gs://([^/]*).*")
	return mb.ReplaceAllString(path, "$1")
}

func ExtrPrefixNameFromGCPPath(path string) string {
	p := strings.TrimSuffix(path, "/")
	p = strings.TrimSuffix(p, "*")
	p = strings.TrimSuffix(p, "*")
	mb := regexp.MustCompile("gs://([^/]*/?)(.*)")
	t := mb.ReplaceAllString(p, "$2")
	if strings.HasSuffix(path, "/") {
		return t + "/"
	} else {
		return t
	}

}

func ExtrObjNameFromPath(path string) string {
	mp := regexp.MustCompile("gs://[^/]*/(.*)")
	return mp.ReplaceAllString(path, "$1")
}

func IsDir(path string) bool {
	return strings.HasPrefix(path, "/")
}

// RemoveBucketNameFromPath gs://x/y/z , will remove gs:/x from string
func RemoveBucketNameFromPath(path string) string {
	m := regexp.MustCompile("gs://[^/]*/?(.*)")
	return m.ReplaceAllString(path, "$1")
}

func RemoveStarsFromRoot(root string) (root_path, prefix string) {
	var pref string
	mb := regexp.MustCompile(`^(.*)/(.*[^\*])+\*+$`)
	if strings.HasSuffix(root, "/**") || strings.HasSuffix(root, "/*") {
		root = mb.ReplaceAllString(root, "$1/$2")
	} else if strings.HasSuffix(root, "**") || strings.HasSuffix(root, "*") {
		pref = mb.ReplaceAllString(root, "$2")
		root = mb.ReplaceAllString(root, "$1")
	}

	root = strings.TrimSuffix(root, "/")

	return root, pref
}

func Direction(in, out string) (string, error) {
	switch {
	case IsBucket(in) && IsBucket(out):
		return "bucket2bucket", nil
	case IsDir(in) && IsDir(out):
		return "local2local", nil
	case IsBucket(in) && IsDir(out):
		return "bucket2local", nil
	case IsDir(in) && IsBucket(out):
		return "local2bucket", nil
	default:
		return "", fmt.Errorf("problem with config, please check again")
	}
}

func PWalkDir(root string, items *Items, wg *sync.WaitGroup) error {
	log.Printf("starting scanning the local directory")

	log.Println("--- root0: ", root)
	root, pref := RemoveStarsFromRoot(root)

	log.Println("--- root1: ", root)
	log.Println("HHEERREE")
	log.Println("--- root2: ", root)
	log.Println("--- pref: ", pref)

	if _, err := os.Stat(root); os.IsNotExist(err) {
		log.Printf("%q does not exist, will create it\n", root)
		wg.Done()
		return nil
	}

	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasPrefix(info.Name(), pref) {
			sn := strings.TrimPrefix(path, root+"/")
			f := Item{Path: sn, Size: info.Size()}
			items.List = append(items.List, f)
		}

		return nil
	})
	if err != nil {
		log.Printf("error walking the path %q: %v\n", root, err)
	}

	sortBySize(items)

	wg.Done()
	log.Printf("found %d the files in local directory\n", len(items.List))
	return nil
}

func WalkBucket(direction string, root string, items *Items, wg *sync.WaitGroup, cred string) error {
	log.Println("starting scanning the bucket")
	bucket := ExtrBucketNameFromPath(root)
	log.Println("--- bucket: ", bucket)
	prefix := ExtrPrefixNameFromGCPPath(root)
	log.Println("--- prefix: ", prefix)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
	defer cancel()

	// TODO make function again
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(cred))
	if err != nil {
		log.Println("error creating a client")
		log.Println(err)
		Pstate.State = "error"
		Pstate.Error = err.Error()
		return err
	}
	defer client.Close()
	bh := client.Bucket(bucket)
	if _, err = bh.Attrs(ctx); err != nil {
		log.Println("can't get bucket attributes")
		log.Println(err)
		Pstate.State = "error"
		Pstate.Error = err.Error()
		return err
	}

	itterObj := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	for i := 0; ; i++ {
		attrs, err := itterObj.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Printf("Bucket(%q).Objects(): %v", bucket, err)
		}

		if !strings.HasSuffix(attrs.Name, "/") {
			var f Item
			if direction == "bucket2local" {
				log.Printf("found file: %s with prefix %s and %s ", attrs.Name, prefix, ExtrDirNameFromObj(prefix))
				f = Item{Path: strings.TrimPrefix(attrs.Name, ExtrDirNameFromObj(prefix)), Size: attrs.Size}
			} else if direction == "local2bucket" {
				log.Printf("found file: %s with prefix %s and %s ", attrs.Name, prefix, ExtrDirNameFromObj(prefix))
				f = Item{Path: strings.TrimPrefix(attrs.Name, prefix), Size: attrs.Size}
			}
			items.List = append(items.List, f)
		}
	}

	sortBySize(items)
	log.Printf("found %d files in the bucket\n", len(items.List))
	wg.Done()
	return nil

}

func sortBySize(items *Items) {
	sort.Slice(items.List, func(i, j int) bool { return items.List[j].Size < items.List[i].Size })
}

func ItemsSum(items Items) (int, int64) {
	var i int
	var f Item
	var s int64
	for i, f = range items.List {
		s = s + f.Size
		i++
	}
	ItemsNumberCurrent = i
	ItemsSizeCurrent = s
	return i, s
}

func FillItemsToTransfer(in Items, out Items, i2t *Items) {
	i2t.List = nil

	checkMap := make(map[string]int64)

	for _, v := range out.List {
		checkMap[v.Path] = v.Size
	}

	for _, v := range in.List {
		if size, ok := checkMap[v.Path]; !ok || size != v.Size {
			i2t.List = append(i2t.List, v)
		}
	}

}

func TransferCheck(p []Item, check Item) Item {
	var v Item
	for _, v = range p {
		if v.Path == check.Path && v.Size == check.Size {
			return Item{}
		}
	}
	return check
}

func Slice2Chan(items Items, c chan Item) {
	for _, v := range items.List {
		c <- v
	}
	close(c)
}

func GetDirsSize(root string, dirs map[string]uint64, ts *uint64, wg *sync.WaitGroup, mu *sync.Mutex) (uint64, error) {
	log.Println("starting scanning the local directory")

	root, pref := RemoveStarsFromRoot(root)
	var dirSize uint64
	if _, err := os.Stat(root); os.IsNotExist(err) {
		log.Printf("%q does not exist\n", root)
		wg.Done()
		return 0, err
	}

	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasPrefix(info.Name(), pref) {
			dirSize = dirSize + uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		log.Printf("error walking the path %q: %v\n", root, err)
	}
	mu.Lock()
	*ts = *ts + dirSize
	dirs[root] = dirSize
	mu.Unlock()
	wg.Done()

	return dirSize, nil
}

func GetOneDiskFreeBytes(mount string) (uint64, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(mount, &stat)
	if err != nil {
		log.Printf("couldn't get disk %s size", mount)
		return 0, err
	}
	free := stat.Bavail * uint64(stat.Bsize)
	return free, nil
}

func SetErrorStateIfNoSpace() error {
	mounts := []string{"/root", "/pse-data-index"}
	for _, v := range mounts {
		f, err := GetOneDiskFreeBytes(v)
		if err != nil {
			Pstate.State = "error"
			err := errors.New("error on mount " + v + ": " + err.Error())
			Pstate.Error = err.Error()
			return err
		}
		if f < (1024 * 1024) {
			Pstate.State = "error"
			err := errors.New("no space left on disk with mount " + v)
			Pstate.Error = err.Error()
			return err
		}
		log.Printf("%d GB left on %s\n", f/1024/1024/1024, v)
	}
	return nil
}
