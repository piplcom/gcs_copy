# gcs_copy
small command line util to transfer files between **local or nfs mount** <==> **gcp bucket**. 
<br/>much faster then gsutil




### usage examples

### Download: 
```console
./gcs_copy  -out /dir1/dir2/ -in gs://BUCKET/obj1/obj2/ -conc 64
```

### Upload:
```console
./gcs_copy  -in /dir1/dir2/ -out gs://BUCKET/obj1/obj2/ -conc 64
```

### flags:
```
  -conc:  concurrent streams (usually 64 is good to utilize max of the system) <br>
          set more for lots of small files <br/><br/>
```
```
  -check: dry run (to check upload total size and files number)
```

get latest compiled exec at: https://github.com/piplcom/gcs_copy/releases 
