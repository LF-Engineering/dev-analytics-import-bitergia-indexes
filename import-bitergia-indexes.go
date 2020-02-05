package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
)

const cDelete string = "DELETE"
const cPut string = "PUT"
const cGet string = "GET"
const cHead string = "HEAD"
const cPost string = "POST"

type esLogPayload struct {
	Msg string    `json:"msg"`
	Dt  time.Time `json:"dt"`
}

type indexData struct {
	Index  string      `json:"_index"`
	Source interface{} `json:"_source"`
}

func toYMDHMSDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

var logURL string

func printf(format string, args ...interface{}) (n int, err error) {
	now := time.Now()
	msg := fmt.Sprintf("%s: "+format, append([]interface{}{toYMDHMSDate(now)}, args...)...)
	n, err = fmt.Printf("%s", msg)
	if logURL != "" {
		err = esLog(logURL, msg, now)
	}
	return
}

func ensureIndex(esURL, index string, init bool) {
	mprintf := printf
	if init {
		mprintf = fmt.Printf
	}
	method := cHead
	url := fmt.Sprintf("%s/%s", esURL, index)
	req, err := http.NewRequest(method, os.ExpandEnv(url), nil)
	if err != nil {
		mprintf("New request error: %+v for %s url: %s\n", err, method, url)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		mprintf("Do request error: %+v for %s url: %s\n", err, method, url)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 200 {
		if resp.StatusCode != 404 {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				mprintf("ReadAll request error: %+v for %s url: %s\n", err, method, url)
				return
			}
			mprintf("Method:%s url:%s status:%d\n%s\n", method, url, resp.StatusCode, body)
			return
		}
		mprintf("Missing %s index, creating\n", index)
		method = cPut
		req, err := http.NewRequest(method, os.ExpandEnv(url), nil)
		if err != nil {
			mprintf("New request error: %+v for %s url: %s\n", err, method, url)
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			mprintf("Do request error: %+v for %s url: %s\n", err, method, url)
			return
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != 200 {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				mprintf("ReadAll request error: %+v for %s url: %s\n", err, method, url)
				return
			}
			mprintf("Method:%s url:%s status:%d\n%s\n", method, url, resp.StatusCode, body)
			return
		}
		mprintf("%s index created\n", index)
	}
}

func esLog(esURL, msg string, dt time.Time) error {
	data := esLogPayload{Msg: msg, Dt: dt}
	index := "import-bitergia-indexes-log"
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("JSON marshall error: %+v for index: %s, data: %+v\n", err, index, data)
		return err
	}
	payloadBody := bytes.NewReader(payloadBytes)
	method := cPost
	url := fmt.Sprintf("%s/%s/_doc", esURL, index)
	req, err := http.NewRequest(method, os.ExpandEnv(url), payloadBody)
	if err != nil {
		fmt.Printf("New request error: %+v for %s url: %s, data: %+v\n", err, method, url, data)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Do request error: %+v for %s url: %s, data: %+v\n", err, method, url, data)
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 201 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("ReadAll request error: %+v for %s url: %s, data: %+v\n", err, method, url, data)
			return err
		}
		fmt.Printf("Method:%s url:%s status:%d, data:%+v\n%s\n", method, url, resp.StatusCode, data, body)
		return err
	}
	return nil
}

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

func getThreadsNum() int {
	// Use environment variable to have singlethreaded version
	st := os.Getenv("ST") != ""
	if st {
		return 1
	}
	nCPUs := 0
	if os.Getenv("NCPUS") != "" {
		n, err := strconv.Atoi(os.Getenv("NCPUS"))
		fatalOnError(err)
		if n > 0 {
			nCPUs = n
		}
	}
	if nCPUs > 0 {
		n := runtime.NumCPU()
		if nCPUs > n {
			nCPUs = n
		}
		runtime.GOMAXPROCS(nCPUs)
		return nCPUs
	}
	nCPUs = runtime.NumCPU()
	runtime.GOMAXPROCS(nCPUs)
	return nCPUs
}

func putJSONData(esURL, index string, payloadBytes []byte) (ok bool) {
	payloadBody := bytes.NewReader(payloadBytes)
	method := cPost
	index = "bitergia-" + index
	url := fmt.Sprintf("%s/%s/_doc", esURL, index)
	req, err := http.NewRequest(method, os.ExpandEnv(url), payloadBody)
	if err != nil {
		printf("New request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		printf("Do request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 201 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			printf("ReadAll request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
			return
		}
		printf("Method:%s url:%s status:%d payload:%+v\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
		return
	}
	ok = true
	return
}

func importJSONFile(dbg bool, esURL, fileName string, maxToken, maxLine int) error {
	file, err := os.Open(fileName)
	fatalOnError(err)
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	// Tweak this if needed
	buf := make([]byte, 0, maxToken*1024)
	scanner.Buffer(buf, maxLine*1024)
	lines := [][]byte{}
	for scanner.Scan() {
		line := []byte(scanner.Text())
		lines = append(lines, line)
	}
	fatalOnError(scanner.Err())
	n := len(lines)
	printf("Processing %d JSONs\n", n)
	processJSON := func(ch chan bool, line []byte) (ok bool) {
		defer func() {
			if ch != nil {
				ch <- ok
			}
		}()
		var data indexData
		fatalOnError(json.Unmarshal(line, &data))
		jsonBytes, err := json.Marshal(data.Source)
		fatalOnError(err)
		if data.Index == "" || len(jsonBytes) == 0 {
			fatalf("Error: empty index name '%s' or JSON payload: '%s' in '%s'\n", data.Index, string(jsonBytes), string(line))
			return
		}
		if dbg {
			printf("payload length %d\n", len(jsonBytes))
		}
		ok = putJSONData(esURL, data.Index, jsonBytes)
		if !ok {
			printf("Error: failed to put JSON '%s' into index 'bitergia-%s'\n", string(jsonBytes), data.Index)
		}
		return
	}
	thrN := getThreadsNum()
	printf("Using %d CPUs\n", thrN)
	statuses := make(map[bool]int)
	statuses[false] = 0
	statuses[true] = 0
	if thrN > 1 {
		ch := make(chan bool)
		nThreads := 0
		for _, line := range lines {
			go processJSON(ch, line)
			nThreads++
			if nThreads == thrN {
				statuses[<-ch]++
				nThreads--
			}
		}
		for nThreads > 0 {
			statuses[<-ch]++
			nThreads--
		}
	} else {
		for _, line := range lines {
			statuses[processJSON(nil, line)]++
		}
	}
	printf("Succeeded: %d, failed: %d\n", statuses[true], statuses[false])
	return nil
}

func importJSONFiles(fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	noLog := os.Getenv("NO_LOG") != ""
	esURL := os.Getenv("ES_URL")
	if esURL == "" {
		esURL = "http://localhost:9200"
	}
	if !noLog {
		ensureIndex(esURL, "import-bitergia-indexes-log", true)
		logURL = esURL
	}
	mts := os.Getenv("MAX_TOKEN")
	maxToken := 2
	if mts != "" {
		mt, err := strconv.Atoi(os.Getenv("MAX_TOKEN"))
		fatalOnError(err)
		if mt > 0 {
			maxToken = mt
		}
	}
	mls := os.Getenv("MAX_LINE")
	maxLine := 2048
	if mls != "" {
		ml, err := strconv.Atoi(os.Getenv("MAX_LINE"))
		fatalOnError(err)
		if ml > 0 {
			maxLine = ml
		}
	}
	if dbg {
		printf("Importing %+v into %s, log: %s, token/line size: %d/%d\n", fileNames, esURL, logURL, maxToken, maxLine)
	}
	n := len(fileNames)
	for i, fileName := range fileNames {
		printf("Importing %d/%d: %s\n", i+1, n, fileName)
		fatalOnError(importJSONFile(dbg, esURL, fileName, maxToken, maxLine))
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		printf("Arguments required: file.json [file2.json [...]]\n")
		return
	}
	dtStart := time.Now()
	fatalOnError(importJSONFiles(os.Args[1:len(os.Args)]))
	dtEnd := time.Now()
	printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
