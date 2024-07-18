package main

import (
	"context"	
	"io"
	"path"

	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	retry "github.com/avast/retry-go"

	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)


func Transfer(args Args, c *chan Item, f func(args Args, wg *sync.WaitGroup, c *chan Item)) {

	var wg sync.WaitGroup
	wg.Add(args.Conc)

	for i := 0; i < args.Conc; i++ {
		go f(args, &wg, c)
	}

	wg.Wait()
	log.Printf("\nDone All\n")

}

func CreateUploadRoutines(args Args, wg *sync.WaitGroup, c *chan Item) {
	bucket := ExtrBucketNameFromPath(args.Out)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
	defer cancel()

	// TODO make function again
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(args.Cred))

	if err != nil {
		log.Errorln("error creating a client")
		log.Errorln(err)
		Pstate.State = "error"
		Pstate.Error = err.Error()
	}

	bh := client.Bucket(bucket)
	if _, err = bh.Attrs(ctx); err != nil {
		log.Errorln("can't get bucket attributes for bucket: ", bucket)
		log.Errorln(err)
		Pstate.State = "error"
		Pstate.Error = err.Error()
	}
	defer client.Close()


	dstPath := RemoveBucketNameFromPath(args.Out)

	for v := range *c {
		err := retry.Do(
			func() error {
				// obj := strings.TrimPrefix(ExtrDirNameFromObj(dstPath)+v.Path, "/")
				obj := strings.TrimPrefix(dstPath+"/"+v.Path, "/")
				log.Println("will transfer: ", obj)
				writer := bh.Object(obj).NewWriter(ctx)
				args.In, _ = RemoveStarsFromRoot(args.In)
				f, err := os.Open(strings.TrimSuffix(args.In, "/") + "/" + v.Path)
				if err != nil {
					log.Errorln("coulnd't open file for writing", strings.TrimSuffix(args.In, "/") + "/" + v.Path)
					log.Errorln(err)
					return err
				}
				w, err := io.Copy(writer, f)
				if err != nil {
					log.Errorf("error copeing file: %s", strings.TrimSuffix(args.In, "/") + "/" + v.Path)
					log.Errorln(err)
					return err
				}
				if w != v.Size {
					log.Printf("expected to transfer file of size %d but got %d", v.Size, w)
					return err
				}

				var m sync.Mutex
				m.Lock()
				ItemsNumberCurrent--
				ItemsSizeCurrent = ItemsSizeCurrent - v.Size
				m.Unlock()

				log.Printf("%d files left to process size is %.2fG",
					ItemsNumberCurrent,
					float64(ItemsSizeCurrent)/1024/1024/1024)

				writer.Close()
				f.Close()
				return nil
			},
			retry.Attempts(5))
		if err != nil {
			log.Errorln(err)
			log.Errorf("errur uploading %s", dstPath+"/"+v.Path)
			Pstate.State = "error"
			Pstate.Error = err.Error()
		}

	}

	wg.Done()

}

func CreateDownloadRoutines(args Args, wg *sync.WaitGroup, c *chan Item) {
	bucket := ExtrBucketNameFromPath(args.In)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*500000)
	defer cancel()

	// TODO make function again
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(args.Cred))
	if err != nil {
		log.Errorln("error creating a client")
		log.Errorln(err)
		Pstate.State = "error"
		Pstate.Error = err.Error()
	}
	bh := client.Bucket(bucket)
	if _, err = bh.Attrs(ctx); err != nil {
		log.Println("can't get bucket attributes for bucket: ", bucket)
		log.Println(err)
		Pstate.State = "error"
		Pstate.Error = err.Error()
	}
	defer client.Close()


	for v := range *c {
		// log.Println("state= ", Pstate.State)
		if Pstate.State == "error" {
			return
		}
		err1 := retry.Do(
			func() error {
				obj := ExtrDirNameFromObj(ExtrObjNameFromPath(args.In)) + v.Path
				log.Println("src: ", strings.TrimSuffix(args.In, "/")+"/"+v.Path)
				log.Println("test: ", ExtrDirNameFromObj(ExtrObjNameFromPath(args.In))+v.Path)
				log.Println("obj: ", obj)
				log.Println("vpath: ", v.Path)
				toMkdir := path.Dir(path.Join(args.Out, v.Path))

				err := os.MkdirAll(toMkdir, os.ModePerm)
				if err != nil {
					log.Errorln("coulnd't create a directory", toMkdir)
					log.Errorln(err)
				}

				reader, err := bh.Object(obj).NewReader(ctx)
				if err != nil {
					log.Errorln("couldn't create reader for: ", obj)
					log.Errorln(err)
				}

				f, err := os.OpenFile(args.Out+"/"+v.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
				if err != nil {
					f.Close()
					log.Errorln(err)
				}
				w, err := io.Copy(f, reader)
				if err != nil {
					log.Errorf("failed to copy a file %s", args.Out+"/"+v.Path)
					log.Errorln(err)
					return err
				}

				err = f.Sync()
				if err != nil {
					log.Errorf("counld't sync file %s to disk", err)
					log.Errorln(err)
					return err
				}

				if w != v.Size {
					log.Errorf("there was a problem downloading file %s",args.Out+"/"+v.Path )
					log.Errorf("expected to transfer file of size %d but got %d", v.Size, w)
					return err
				}

				var m sync.Mutex
				m.Lock()
				ItemsNumberCurrent--
				ItemsSizeCurrent = ItemsSizeCurrent - v.Size
				m.Unlock()

				log.Infof("\r\033[K%d files of total size %.2fG left to process",
					ItemsNumberCurrent,
					float64(ItemsSizeCurrent)/1024/1024/1024)

				f.Close()
				reader.Close()

				return nil

			},
			retry.Attempts(2))
		if err1 != nil {
			log.Errorf("error downloading file %s", args.Out+"/"+v.Path)
			log.Errorln(err1.Error())
			Pstate.State = "error"
			Pstate.Error = err1.Error()
			log.Infof("state=%s\n", Pstate.State)
			return
		}
	}
	wg.Done()

}
