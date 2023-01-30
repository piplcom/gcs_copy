package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/piplcom/gcs_copy/conf"
	ppaths "github.com/piplcom/gcs_copy/paths"
	log "github.com/sirupsen/logrus"
)

func handleRunCopy(w http.ResponseWriter, r *http.Request) {
	var Args = conf.Args{
		Conc: conc,
		In:   in,
		Out:  out,
		Cred: cred,
	}

	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&Args)
	if err != nil {
		log.Error(err)
	}
	log.Println(Args)
	runCopy(Args)

	fmt.Printf("---------------------------------------------\n")
	fmt.Printf("credential: %s\t\t\n", cred)
	fmt.Printf("input:      %s\t\t\n", in)
	fmt.Printf("output:     %s\t\t\n", out)
	fmt.Printf("concurrent workers:     %d\t\t\n", conc)
	fmt.Printf("---------------------------------------------\n\n")
}

func handleGetStatus(w http.ResponseWriter, r *http.Request) {
	type State struct {
		ItemsNumberCurrent int
		ItemsSizeCurrent   int64
	}
	data := State{
		ItemsNumberCurrent: ppaths.ItemsNumberCurrent,
		ItemsSizeCurrent:   ppaths.ItemsSizeCurrent,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(data)
}
