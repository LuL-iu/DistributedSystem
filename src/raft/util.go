package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = true

var logger *log.Logger

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Printf(format, a...)
	}
	return
}

func init() {
	file, err := os.Create("../logs/raft.log")
	if err != nil {
		log.Fatal(err)
	}
	logger = log.New(file, "", log.LstdFlags|log.Lmicroseconds)

	log.SetFlags(log.Llongfile)
	log.SetPrefix("log.Fatal: ")
}

func Min(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}
