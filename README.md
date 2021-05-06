# gcs_copy
small command line util to transfer files between mount and gcp bucket. much faster then gsutil




### for regular uploads/downloads don't provide "data" flag (only for tdime for now)

### Download: 
```console
./gcs_copy  -out /dir1/dir2/ -in gs://BUCKET/obj1/obj2/ -conc 128
```

### Upload:
```console
./gcs_copy  -in /dir1/dir2/ -out gs://BUCKET/obj1/obj2/ -conc 128
```

flags:
  -conc: cocurrency
  -check: dry run (to check upload total size and files number)
