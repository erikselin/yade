package main

import (
	"bufio"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
)

const (
	argDot     = "dot"
	argDry     = "dry"
	argVersion = "version"
	argWorkers = "workers"
)

var (
	defaultDot     = false
	defaultDry     = false
	defaultWorkers = runtime.NumCPU()
	version        = "unknown"
)

var (
	dot         bool
	dry         bool
	showVersion bool
	workers     int
)

// init ...
func init() {
	flag.BoolVar(&dot, argDot, defaultDot, "")
	flag.BoolVar(&dry, argDry, defaultDry, "")
	flag.BoolVar(&showVersion, argVersion, false, "")
	flag.IntVar(&workers, argWorkers, defaultWorkers, "")
	flag.Usage = usage
}

// main ...
func main() {
	flag.Parse()
	if len(os.Args) <= 1 {
		usage()
		os.Exit(1)
	}
	if showVersion {
		fmt.Println(version)
		os.Exit(0)
	}
	if len(flag.Args()) != 1 {
		usage()
		os.Exit(1)
	}
	dagFile := flag.Args()[0]
	d, err := newDag(dagFile)
	if err != nil {
		log.Fatal(err)
	}
	if dot {
		if err = d.writeDot(os.Stdout); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}
	if err := os.Chdir(path.Dir(dagFile)); err != nil {
		log.Fatal(err)
	}
	if dry {
		for _, t := range d.Tasks {
			t.skip = true
		}
	}
	log.Print("")
	log.Print("o   o   O  o-o   o--o ")
	log.Print(" \\ /   / \\ |  \\  |    ")
	log.Print("  O   o---o|   O O-o  ")
	log.Print("  |   |   ||  /  |    ")
	log.Print("  o   o   oo-o   o--o")
	log.Print("")
	log.Print("Starting Yet Another Dag Executor")
	log.Print("")
	log.Printf("Dag: %s", dagFile)
	log.Printf("Dry: %t", dry)
	log.Printf("Workers: %d", workers)
	log.Print("")
	if err = validateDag(d); err != nil {
		log.Fatal(err)
	}
	log.Print("")
	if err := run(d, dagFile); err != nil {
		log.Print("")
		log.Fatal(err)
	}
	log.Print("")
	log.Print("Success")
}

// usage ...
func usage() {
	fmt.Printf("usage: yade [--help] [--%s] <arguments> file\n", argVersion)
	fmt.Printf(" --%s            Output a dot graph of the DAG without executing anything (default: %t)\n", argDot, defaultDot)
	fmt.Printf(" --%s            Run the DAG without executing any commands (default: %t)\n", argDry, defaultDry)
	fmt.Printf(" --%s <num>  Number of parallel workers (default: %d)\n", argWorkers, defaultWorkers)
}

// validate ...
func validateDag(d *dag) error {
	if d.Name == "" {
		return errors.New("[\u001b[31mFAIL\u001b[0m] dag name attribute must be set")
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' name", d.Name)
	if len(d.Tasks) == 0 {
		return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] dag '%s' has no tasks", d.Name)
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' task count", d.Name)
	taskNames := map[string]struct{}{}
	for _, t := range d.Tasks {
		if t.Name == "" {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] dag '%s' has a task without a name attribute", d.Name)
		}
		log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' name", t.Name)
		if t.Cmd == "" {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' has no command set", t.Name)
		}
		log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' command", t.Name)
		if _, exists := taskNames[t.Name]; exists {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] two tasks have the same name '%s'", t.Name)
		}
		log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' uniqueness", t.Name)
		taskNames[t.Name] = struct{}{}
	}
	for _, t := range d.Tasks {
		for _, f := range t.Files {
			if _, err := os.Stat(f); os.IsNotExist(err) {
				return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' requires missing file '%s'", t.Name, f)
			}
			log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' file '%s'", t.Name, f)
		}
		for _, dep := range t.Deps {
			if _, exists := taskNames[dep]; !exists {
				return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' depends on missing task '%s'", t.Name, dep)
			}
			if dep == t.Name {
				return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' depends on itself", t.Name)
			}
			log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' dependency '%s'", t.Name, dep)
		}
	}
	if err := d.circularDependencyCheck(); err != nil {
		return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] %s", err)
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' for circular dependency", d.Name)
	return nil
}

func run(d *dag, dagFile string) error {
	d.exec(workers)
	errors := d.listErrors()
	if len(errors) > 0 {
		for _, err := range errors {
			log.Print(err)
		}
		f, err := ioutil.TempFile(".", "retry-*.xml")
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		enc := xml.NewEncoder(f)
		enc.Indent("", "  ")
		if err := enc.Encode(d); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		return fmt.Errorf("Failure (resumable dag stored in %s)", path.Join(path.Dir(dagFile), f.Name()))
	}
	return nil
}
