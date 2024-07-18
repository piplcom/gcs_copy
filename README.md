# gcs_copy

small command line util to transfer files between **local or nfs mount** <==> **gcp bucket**.

much faster then gsutil

get latest compiled exec at: <https://github.com/piplcom/gcs_copy/releases>

## usage examples

## Run As A Service

```console
./gcs_copy -api -port 8082 -bindip '0.0.0.0'
```

### to start copy send POST request to /run endpoing

```json
{
    "Conc": 2,
    "In":   "gs://bucketx/test",
    "Out":  "/bigdir/test6"
    "Check": true // for dry run (to get sizes to copy) default false
}
```

### for regex in directory use `**`

/tmp/testdir/** will work same as /tmp/testdir/ (without blob)

```json
{
    "Conc": 64,
    "In":   "/tmp/testdir/files**",
    "Out":  "gs://bucket66/test1"
    "Check": true // for dry run (to get sizes to copy) default false
}
```

to get status GET /state </br></br>

to get dirs sizes GET /size (no regex!):

```json
[
    "/Users/yosef.yudilevich/git/gcs_copy/test/aa",
    "/Users/yosef.yudilevich/git/gcs_copy/test/bb"
]
```

## Run As CLI

### Download

```console
./gcs_copy -out /dir1/dir2/ -in gs://BUCKET/obj1/obj2/ -conc 64
```

### Upload

```console
./gcs_copy  -in /dir1/dir2/ -out gs://BUCKET/obj1/obj2/ -conc 64
```

### flags

```txt
  -conc:  concurrent streams (usually 64 is good to utilize max of the system)
          set more for lots of small files
```

```txt
  -check: dry run (to check upload total size and files number)
```

using nfs as local mount
set this option in nfs mount options

```txt
nconnect=16
```

( from: <https://www.suse.com/support/kb/doc/?id=000019933> )
