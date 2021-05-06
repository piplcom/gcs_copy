package paths

import (
	"context"
	"fmt"
	// "log"
	"regexp"
	"sort"
	"sync"
	"time"
	"os"

	// "strconv"
	"strings"

	"io/fs"
	"path/filepath"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	log "github.com/sirupsen/logrus"
)

var (
	FilesChan          		= make(chan Item)
	GcpObjectsChan     		= make(chan Item)
	ItemsToTransferChan		= make(chan Item)
	AllFiles           		Items
	AllObjects         		Items
	ItemsToTransfer    		Items
	ItemsNumberCurrent 		int
	ItemsSizeCurrent   		int64
)

type Item struct {
	Path string
	Size int64
}

type Items struct {
	List []Item
}

func IsBucket(path string) bool {
	return strings.HasPrefix(path, "gs://")
}

func ExtrBucketNameFromPath(path string) string {
	mb := regexp.MustCompile("gs://([^/]*).*")
	return mb.ReplaceAllString(path, "$1")
}

func ExtrPrefixNameFromGCPPath(path string) string {
	mb := regexp.MustCompile("gs://([^/]*/?)(.*)")
	return mb.ReplaceAllString(path, "$2")
	
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

// func tdimeFilesList() []File {
// 	var l []File
// 	for i := 0; i <= 255; i++ {
// 		for j := 0; j <= 4095; j++ {
// 			a := fmt.Sprintf("/%02s/%03s", strconv.FormatInt(int64(i), 16), strconv.FormatInt(int64(j), 16))
// 			l = append(l, File{a, 1})
// 		}
// 	}
// 	return l
// }

func PWalkDir(root string, items *Items, wg *sync.WaitGroup) error {
	// Check if dir exists at all
	if _, err := os.Stat(root); os.IsNotExist(err) {
		fmt.Printf("%q does not exist, will create it\n", root)
		wg.Done()
		return nil
	}

	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() {
			sn := strings.TrimPrefix(path, root+"/")
			f := Item{Path: sn, Size: info.Size()}
			items.List = append(items.List, f)
		}

		return nil
	})
	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", root, err)
	}

	sortBySize(items)

	wg.Done()
	fmt.Printf("found %d the files in local directory\n",len(items.List))
	return nil
}

func WalkBucket(root string, items *Items, wg *sync.WaitGroup, cred string) error {

	bucket := ExtrBucketNameFromPath(root)
	prefix := ExtrPrefixNameFromGCPPath(root)


	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
	defer cancel()
	defer cancel()

	// TODO make function again
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(cred))
	if err != nil {
		log.Fatalln("error creating a client: ", err)
		return err
	}
	defer client.Close()
	bh := client.Bucket(bucket)
	if _, err = bh.Attrs(ctx); err != nil {
		log.Fatalln("can't get bucket attributes: ", err)
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
			fmt.Printf("Bucket(%q).Objects(): %v", bucket, err)
		}
		if !strings.HasSuffix(attrs.Name, "/") {
			// first we put the paths in slice , putting stright to
			// channel doesn't work good
			// fmt.Printf("found : %q of size %d\n",strings.TrimPrefix(attrs.Name, prefix+"/"),attrs.Size)
			f := Item{Path: strings.TrimPrefix(attrs.Name, prefix+"/"), Size: attrs.Size}
			items.List = append(items.List, f)

			// Sorting files by size from big to small
			// sort.Slice(items.List, func(i, j int) bool { return items.List[j].Size < items.List[i].Size })
			sortBySize(items)

		}

	}




	fmt.Printf("found %d the files in the bucket\n",len(items.List))
	wg.Done()

	return nil

}

func sortBySize(items *Items){
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

func FillItemsToTransfer(in Items, out Items) {

	for _, v := range in.List {
		var a = TransferCheck(out.List, v)
		if a.Path != "" {
			ItemsToTransfer.List = append(ItemsToTransfer.List, a)
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
	// log.Infoln("in Slice2CHan")
	for _, v := range items.List {
		c <- v
	}
	close(c)
}