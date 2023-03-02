package transfer

import (
	"context"
	"io"
	"path"

	// "log"
	"os"

	// "regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	retry "github.com/avast/retry-go"

	// "github.com/pkg/errors"
	"github.com/piplcom/gcs_copy/conf"
	ppaths "github.com/piplcom/gcs_copy/paths"
	"google.golang.org/api/option"

	// "github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
)

// func Transfer(log *log.Logger, args conf.Args, direction string) {
func Transfer(args conf.Args, c *chan ppaths.Item, f func(args conf.Args, wg *sync.WaitGroup, c *chan ppaths.Item)) {

	var wg sync.WaitGroup
	wg.Add(args.Conc)

	for i := 0; i < args.Conc; i++ {
		go f(args, &wg, c)
	}

	// Option for progress bar
	// max := ppaths.ItemsSizeCurrent
	// bar := progressbar.Default(max)
	// for i := int64(0); i < max;{
	// 	i = max - ppaths.ItemsSizeCurrent
	// 	bar.Set64(max - ppaths.ItemsSizeCurrent)
	// 	time.Sleep(time.Second)
	// }

	wg.Wait()

	log.Printf("\nDone All\n")

}

func CreateUploadRoutines(args conf.Args, wg *sync.WaitGroup, c *chan ppaths.Item) {
	bucket := ppaths.ExtrBucketNameFromPath(args.Out)
	// log.Infoln("bucket before: ", bucket)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
	defer cancel()

	// TODO make function again
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(args.Cred))

	if err != nil {
		log.Fatalln("error creating a client: ", err)
	}
	bh := client.Bucket(bucket)
	if _, err = bh.Attrs(ctx); err != nil {
		log.Fatalln("can't get bucket attributes: ", err)
	}
	defer client.Close()
	//

	dstPath := ppaths.RemoveBucketNameFromPath(args.Out)

	for v := range *c {
		err := retry.Do(
			func() error {
				obj := strings.TrimPrefix(dstPath+"/"+v.Path, "/")
				log.Println("will transfer: ", obj)
				writer := bh.Object(obj).NewWriter(ctx)
				args.In, _ = ppaths.RemoveStarsFromRoot(args.In)
				f, err := os.Open(strings.TrimSuffix(args.In, "/") + "/" + v.Path)
				if err != nil {
					log.Println(err)
					return err
				}
				w, err := io.Copy(writer, f)
				if err != nil {
					log.Println(err)
					return err
				}
				if w != v.Size {
					log.Printf("expected to transfer file of size %d but got %d", v.Size, w)
					return err
				}

				var m sync.Mutex
				m.Lock()
				ppaths.ItemsNumberCurrent--
				ppaths.ItemsSizeCurrent = ppaths.ItemsSizeCurrent - v.Size
				m.Unlock()

				log.Printf("\r\033[K%d files left to process size is %.2fG",
					ppaths.ItemsNumberCurrent,
					float64(ppaths.ItemsSizeCurrent)/1024/1024/1024)

				writer.Close()
				f.Close()
				return nil
			},
			retry.Attempts(5))
		if err != nil {
			log.Println(err)
		}

	}

	wg.Done()

}

func CreateDownloadRoutines(args conf.Args, wg *sync.WaitGroup, c *chan ppaths.Item) {
	bucket := ppaths.ExtrBucketNameFromPath(args.In)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
	defer cancel()

	// TODO make function again
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(args.Cred))
	if err != nil {
		log.Fatalln("error creating a client: ", err)
	}
	bh := client.Bucket(bucket)
	if _, err = bh.Attrs(ctx); err != nil {
		log.Fatalln("can't get bucket attributes: ", err)
	}
	defer client.Close()
	//
	// log.Println("reading from chan: ", <-*c)

	for v := range *c {

		err := retry.Do(
			func() error {

				// obj := ppaths.ExtrObjNameFromPath()
				obj := ppaths.ExtrObjNameFromPath(strings.TrimSuffix(args.In, "/") + "/" + v.Path)
				toMkdir := path.Dir(path.Join(args.Out, v.Path))

				err := os.MkdirAll(toMkdir, os.ModePerm)
				if err != nil {
					log.Println(err)
				}

				reader, err := bh.Object(obj).NewReader(ctx)
				if err != nil {
					log.Errorln(err)
				}

				f, err := os.OpenFile(args.Out+"/"+v.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
				if err != nil {
					f.Close()
					log.Println(err)
				}
				w, err := io.Copy(f, reader)
				if err != nil {
					log.Fatalln(err)
					return err
				}

				err = f.Sync()
				if err != nil {
					log.Fatalln("counld't sync file to disk", err)
					return err
				}

				if w != v.Size {
					log.Printf("expected to transfer file of size %d but got %d", v.Size, w)
					return err
				}

				var m sync.Mutex
				m.Lock()
				ppaths.ItemsNumberCurrent--
				ppaths.ItemsSizeCurrent = ppaths.ItemsSizeCurrent - v.Size
				m.Unlock()

				log.Printf("\r\033[K%d files left to process size is %.2fG",
					ppaths.ItemsNumberCurrent,
					float64(ppaths.ItemsSizeCurrent)/1024/1024/1024)

				f.Close()
				reader.Close()

				return nil

			},
			retry.Attempts(5))
		if err != nil {
			log.Println(err)
		}
	}
	wg.Done()

}
