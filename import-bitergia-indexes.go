package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

func importJSONfiles(fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	if dbg {
		fmt.Printf("Importing %+v\n", fileNames)
	}
	n := len(fileNames)
	for i, fileName := range fileNames {
		fmt.Printf("Importing %d/%d: %s\n", i+1, n, fileName)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Arguments required: file.json [file2.json [...]]\n")
		return
	}
	dtStart := time.Now()
	fatalOnError(importJSONfiles(os.Args[1:len(os.Args)]))
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
