package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/starlinglab/integrity-v2/dummy"
)

// Main file for all-in-one build

func main() {
	if len(os.Args) == 1 {
		fmt.Println("TODO help text")
		return
	}

	// Try to run command based on binary name
	// Might have been symlinked with different names
	ok := run(filepath.Base(os.Args[0]), os.Args[1:])
	if !ok {
		// If that failed, then use the second arg: ./integrity-v2 dummy ...
		if len(os.Args) == 1 {
			fmt.Fprintln(os.Stderr, "unknown command")
			os.Exit(1)
		}
		ok = run(os.Args[1], os.Args[2:])
	}
	if !ok {
		// If that fails too then give up
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(1)
	}
}

func run(cmd string, args []string) bool {
	switch cmd {
	case "dummy":
		dummy.Run(args)
	default:
		// Unknown command
		return false
	}
	return true
}
