package paths

import (
	"context"
	"fmt"

	// "log"
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
	log "github.com/sirupsen/logrus"
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

func IsBucket(path string) bool {
	return strings.HasPrefix(path, "gs://")
}

func ExtrBucketNameFromPath(path string) string {
	mb := regexp.MustCompile("gs://([^/]*).*")
	return mb.ReplaceAllString(path, "$1")
}

func ExtrPrefixNameFromGCPPath(path string) string {
	p := strings.TrimSuffix(path, "/")
	mb := regexp.MustCompile("gs://([^/]*/?)(.*)")
	return mb.ReplaceAllString(p, "$2")

}

func ExtrObjNameFromPath(path string) string {
	p := strings.TrimSuffix(path, "/")
	mp := regexp.MustCompile("gs://[^/]*/(.*)")
	return mp.ReplaceAllString(p, "$1")
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
	if strings.HasSuffix(root, "/**") || strings.HasSuffix(root, "/*") {
		mb := regexp.MustCompile(`^(.*)/(.*[^\*])+\*+$`)
		root = mb.ReplaceAllString(root, "$1")
	} else if strings.HasSuffix(root, "**") || strings.HasSuffix(root, "*") {
		mb := regexp.MustCompile(`^(.*)/(.*[^\*])+\*+$`)
		pref = mb.ReplaceAllString(root, "$2")
		root = mb.ReplaceAllString(root, "$1")
	}
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
	log.Println("starting scanning the local directory")
	// Check if dir exists at all
	// var pref string //prefix for glob

	// if strings.HasSuffix(root, "/**") || strings.HasSuffix(root, "/*") {
	// 	mb := regexp.MustCompile(`^(.*)/(.*[^\*])+\*+$`)
	// 	root = mb.ReplaceAllString(root, "$1")
	// } else if strings.HasSuffix(root, "**") || strings.HasSuffix(root, "*") {
	// 	mb := regexp.MustCompile(`^(.*)/(.*[^\*])+\*+$`)
	// 	pref = mb.ReplaceAllString(root, "$2")
	// 	root = mb.ReplaceAllString(root, "$1")
	// }

	root, pref := RemoveStarsFromRoot(root)

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
		fmt.Printf("error walking the path %q: %v\n", root, err)
	}

	sortBySize(items)

	wg.Done()
	log.Printf("found %d the files in local directory\n", len(items.List))
	return nil
}

func WalkBucket(root string, items *Items, wg *sync.WaitGroup, cred string) error {
	log.Println("starting scanning the bucket")
	bucket := ExtrBucketNameFromPath(root)
	prefix := ExtrPrefixNameFromGCPPath(root)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
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
	// itterObj.PageInfo().MaxSize = 10000
	// fmt.Println(fmt.Printf("%+v\n", itterObj.PageInfo()))
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
		}
	}

	// Sorting files by size from big to small
	// sort.Slice(items.List, func(i, j int) bool { return items.List[j].Size < items.List[i].Size })
	sortBySize(items)

	log.Printf("found %d the files in the bucket\n", len(items.List))
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

	// log.Println("started adding diff to list")
	for _, v := range in.List {

		if size, ok := checkMap[v.Path]; !ok || size != v.Size {
			// log.Printf("adding  to list because %d != %d",size, v.Size)
			i2t.List = append(i2t.List, v)
			//do something here
		}
		// var a = TransferCheck(out.List, v)
		// if a.Path != "" {
		// 	ItemsToTransfer.List = append(ItemsToTransfer.List, a)
		// }

	}
	// log.Println("done adding diff to list")

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
		fmt.Printf("%q does not exist\n", root)
		wg.Done()
		return 0, err
	}

	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
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
		fmt.Printf("error walking the path %q: %v\n", root, err)
	}
	mu.Lock()
	*ts = *ts + dirSize
	dirs[root] = dirSize
	mu.Unlock()
	wg.Done()
	// log.Printf("the size of dir is %d\n", dirSize)
	return dirSize, nil
}
