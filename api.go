package main

import (
	"encoding/json"
	"net/http"
	"sync"

	// "github.com/piplcom/gcs_copy/conf"
	log "github.com/sirupsen/logrus"
)

func handleRunCopy(w http.ResponseWriter, r *http.Request) {
	var Args = Args{
		Conc:  conc,
		In:    in,
		Out:   out,
		Cred:  cred,
		Check: check,
	}

	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&Args)
	if err != nil {
		log.Error(err)
	}
	log.Println(Args)
	Pstate.State = "running"
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	go runCopy(Args)
}

func handleSize(w http.ResponseWriter, r *http.Request) {

	type Data struct {
		Dirs  map[string]uint64
		Total uint64
	}

	var data Data

	decoder := json.NewDecoder(r.Body)
	var sizePaths []string
	err := decoder.Decode(&sizePaths)
	if err != nil {
		log.Error(err)
	}
	var totalSize uint64
	var mu sync.Mutex
	var walkWg sync.WaitGroup
	dirs := make(map[string]uint64)
	walkWg.Add(len(sizePaths))
	for _, v := range sizePaths {

		// fmt.Fprintf(w, "%s\n", v)
		go GetDirsSize(v, dirs, &totalSize, &walkWg, &mu)
		if err != nil {
			log.Error(err)
		}

	}
	walkWg.Wait()
	data.Dirs = dirs
	data.Total = totalSize

	log.Printf("%v", dirs)
	log.Printf("the total size is is %d\n", totalSize)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(data)
}

func handleGetStatus(w http.ResponseWriter, r *http.Request) {
	// type State struct {
	// 	ItemsNumberCurrent int
	// 	ItemsSizeCurrent   int64
	// }

	data := State{
		ItemsNumberCurrent: ItemsNumberCurrent,
		ItemsSizeCurrent:   ItemsSizeCurrent,
		State:              Pstate.State,
		Error:              Pstate.Error,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(data)
}
