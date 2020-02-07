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

func progressInfo(i, n int, start time.Time, last *time.Time, period time.Duration) {
	now := time.Now()
	if last.Add(period).Before(now) {
		perc := 0.0
		if n > 0 {
			perc = (float64(i) * 100.0) / float64(n)
		}
		eta := start
		if i > 0 && n > 0 {
			etaNs := float64(now.Sub(start).Nanoseconds()) * (float64(n) / float64(i))
			etaDuration := time.Duration(etaNs) * time.Nanosecond
			eta = start.Add(etaDuration)
			printf("%d/%d (%.3f%%), ETA: %v\n", i, n, perc, eta)
		}
		*last = now
	}
}

func putJSONMapping(esURL, index string, payloadBytes []byte, quiet bool) (ok bool) {
	payloadBody := bytes.NewReader(payloadBytes)
	method := cPut
	index = "bitergia-" + index
	url := fmt.Sprintf("%s/%s/_mapping", esURL, index)
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
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			printf("ReadAll request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
			return
		}
		if !quiet {
			printf("Method:%s url:%s status:%d payload:%+v\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
		}
		return
	}
	ok = true
	return
}

func putJSONData(esURL, index string, payloadBytes []byte, quiet bool) (ok bool) {
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
		if !quiet {
			printf("Method:%s url:%s status:%d payload:%+v\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
		}
		return
	}
	ok = true
	return
}

func bulkJSONData(esURL, index string, payloadBytes []byte, quiet bool) (ok bool) {
	payloadBody := bytes.NewReader(payloadBytes)
	method := cPost
	index = "bitergia-" + index
	//url := fmt.Sprintf("%s/%s/_bulk?refresh=wait_for", esURL, index)
	url := fmt.Sprintf("%s/%s/_bulk", esURL, index)
	req, err := http.NewRequest(method, os.ExpandEnv(url), payloadBody)
	if err != nil {
		printf("New request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		printf("Do request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			printf("ReadAll request error: %+v for %s url: %s, payload: %s\n", err, method, url, string(payloadBytes))
			return
		}
		if !quiet {
			printf("Method:%s url:%s status:%d payload:%+v\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
		}
		return
	}
	ok = true
	return
}

func importJSONFile(dbg bool, esURL, fileName string, maxToken, maxLine, bulkSize int, allowMapFail, allowDataFail bool) error {
	contents, err := ioutil.ReadFile(fileName + ".map")
	if err != nil {
		printf("Failed to read mapping file for '%s': %+v, it may sometimes work, but please see README.md\n", fileName, err)
	} else {
		var iunknown interface{}
		fatalOnError(json.Unmarshal(contents, &iunknown))
		dta, u := iunknown.(map[string]interface{})
		if !u {
			fatalf("cannot unmarshal %v into list of index mappings\n", iunknown)
		}
		for index, mapp := range dta {
			ms, u := mapp.(map[string]interface{})
			if !u {
				fatalf("cannot unmarshal %v into mappings (index %s)\n", mapp, index)
			}
			m, u := ms["mappings"]
			if !u {
				fatalf("no 'mappings' property in %v (index %s)\n", ms, index)
			}
			is, u := m.(map[string]interface{})
			if !u {
				fatalf("cannot unmarshal %v into items (index %s)\n", m, index)
			}
			i, u := is["items"]
			if !u {
				i, u = is["item"]
				if !u {
					fatalf("no 'items' or 'item' properties in %v (index %s)\n", is, index)
				}
			}
			pl := make(map[string]interface{})
			mi, u := i.(map[string]interface{})
			skipKeys := map[string]struct{}{"_all": {}}
			for k, d := range mi {
				_, skip := skipKeys[k]
				if skip {
					continue
				}
				pl[k] = d
			}
			jsonBytes, err := json.Marshal(pl)
			fatalOnError(err)
			ensureIndex(esURL, "bitergia-"+index, false)
			ok := putJSONMapping(esURL, index, jsonBytes, allowMapFail)
			if !ok {
				if allowMapFail {
					printf("Failed to put JSON mappings into index 'bitergia-%s' (file %s)\n", index, fileName+".map")
				} else {
					fatalf("Error: failed to put JSON mappings '%s' into index 'bitergia-%s'\n", string(jsonBytes), index)
				}
			}
			printf("%s mapping created\n", fileName)
		}
	}

	file, err := os.Open(fileName)
	fatalOnError(err)
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	// Tweak this if needed
	buf := make([]byte, 0, maxToken*1024)
	scanner.Buffer(buf, maxLine*1024)
	lines := [][]byte{}
	buckets := [][][]byte{}
	bucket := [][]byte{}
	nBuckets := 0
	bulk := false
	if bulkSize > 1 {
		bulk = true
	}
	bs := 0
	for scanner.Scan() {
		line := []byte(scanner.Text())
		lines = append(lines, line)
		if bulk {
			bucket = append(bucket, line)
			bs++
			if bs == bulkSize {
				buckets = append(buckets, bucket)
				bucket = [][]byte{}
				bs = 0
			}
		}
	}
	if bulk {
		if bs > 0 {
			buckets = append(buckets, bucket)
		}
		nBuckets = len(buckets)
		printf("%d buckets up to %d JSONs each\n", nBuckets, bulkSize)
	}
	fatalOnError(scanner.Err())
	n := len(lines)
	if bulk {
		printf("Processing %d JSONs in %d buckets\n", n, nBuckets)
	} else {
		printf("Processing %d JSONs\n", n)
	}
	processJSON := func(ch chan bool, lineNo int, line []byte) (ok bool) {
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
		ok = putJSONData(esURL, data.Index, jsonBytes, allowDataFail)
		if !ok {
			if allowDataFail {
				printf("Failed to put line %d JSON data into index 'bitergia-%s' (file %s)\n", lineNo, data.Index, fileName)
			} else {
				fatalf("Error: failed to put line %d JSON '%s' into index 'bitergia-%s'\n", lineNo, string(jsonBytes), data.Index)
			}
		}
		return
	}
	processBucket := func(ch chan bool, bucket [][]byte) (ok bool) {
		defer func() {
			if ch != nil {
				ch <- ok
			}
		}()
		index := ""
		bulkOp := []byte("")
		payloads := []byte{}
		newLine := []byte("\n")
		for _, line := range bucket {
			var data indexData
			fatalOnError(json.Unmarshal(line, &data))
			jsonBytes, err := json.Marshal(data.Source)
			fatalOnError(err)
			if data.Index == "" || len(jsonBytes) == 0 {
				fatalf("Error: empty index name '%s' or JSON payload: '%s' in '%s'\n", data.Index, string(jsonBytes), string(line))
				return
			}
			if index == "" {
				index = data.Index
				bulkOp = []byte("{\"index\": {\"_index\":\"bitergia-" + index + "\"}}\n")
				payloads = bulkOp
			} else {
				if data.Index != index {
					fatalf("Error: non unique index '%s' != '%s' in '%s'\n", data.Index, index, string(line))
				}
				payloads = append(payloads, bulkOp...)
			}
			payloads = append(payloads, jsonBytes...)
			payloads = append(payloads, newLine...)
		}
		ok = bulkJSONData(esURL, index, payloads, allowDataFail)
		if !ok {
			if allowDataFail {
				printf("Failed to bulk put JSON data into index 'bitergia-%s' (file %s)\n", index, fileName)
			} else {
				fatalf("Error: failed to bulk put JSON into index 'bitergia-%s'\n", index)
			}
		}
		return
	}
	thrN := getThreadsNum()
	printf("Using %d CPUs\n", thrN)
	statuses := make(map[bool]int)
	statuses[false] = 0
	statuses[true] = 0
	processed := 0
	all := 0
	if bulk {
		all = len(buckets)
	} else {
		all = len(lines)
	}
	lastTime := time.Now()
	dtStart := lastTime
	freq := time.Duration(30) * time.Second
	if thrN > 1 {
		ch := make(chan bool)
		nThreads := 0
		if bulk {
			for _, bucket := range buckets {
				go processBucket(ch, bucket)
				nThreads++
				if nThreads == thrN {
					statuses[<-ch]++
					nThreads--
					processed++
					progressInfo(processed, all, dtStart, &lastTime, freq)
				}
			}
		} else {
			for lineNo, line := range lines {
				go processJSON(ch, lineNo, line)
				nThreads++
				if nThreads == thrN {
					statuses[<-ch]++
					nThreads--
					processed++
					progressInfo(processed, all, dtStart, &lastTime, freq)
				}
			}
		}
		for nThreads > 0 {
			statuses[<-ch]++
			nThreads--
			processed++
			progressInfo(processed, all, dtStart, &lastTime, freq)
		}
	} else {
		if bulk {
			for _, bucket := range buckets {
				statuses[processBucket(nil, bucket)]++
				processed++
				progressInfo(processed, all, dtStart, &lastTime, freq)
			}
		} else {
			for lineNo, line := range lines {
				statuses[processJSON(nil, lineNo, line)]++
				processed++
				progressInfo(processed, all, dtStart, &lastTime, freq)
			}
		}
	}
	printf("Succeeded: %d, failed: %d\n", statuses[true], statuses[false])
	return nil
}

func importJSONFiles(fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	noLog := os.Getenv("NO_LOG") != ""
	allowMapFail := os.Getenv("ALLOW_MAP_FAIL") != ""
	allowDataFail := os.Getenv("ALLOW_DATA_FAIL") != ""
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
	maxLine := 16384
	if mls != "" {
		ml, err := strconv.Atoi(os.Getenv("MAX_LINE"))
		fatalOnError(err)
		if ml > 0 {
			maxLine = ml
		}
	}
	bss := os.Getenv("BULK_SIZE")
	bulkSize := 1000
	if bss != "" {
		bs, err := strconv.Atoi(os.Getenv("BULK_SIZE"))
		fatalOnError(err)
		if bs > 0 {
			bulkSize = bs
		}
	}
	printf("Importing %+v into %s, log: %s, token/line/bulk size: %d/%d/%d, allow map/data fail: %v/%v\n", fileNames, esURL, logURL, maxToken, maxLine, bulkSize, allowMapFail, allowDataFail)
	n := len(fileNames)
	for i, fileName := range fileNames {
		printf("Importing %d/%d: %s\n", i+1, n, fileName)
		fatalOnError(importJSONFile(dbg, esURL, fileName, maxToken, maxLine, bulkSize, allowMapFail, allowDataFail))
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
