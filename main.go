package main

import (
	"bufio"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"sync"
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
	f, err := os.Open(dagFile)
	if err != nil {
		log.Fatal(err)
	}
	d, err := newDag(f)
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
	if err = d.validate(); err != nil {
		log.Fatal(err)
	}
	log.Print("")
	retry := fmt.Sprintf("%s.retry", path.Base(strings.Replace(dagFile, path.Ext(dagFile), "", -1)))
	if err := d.run(workers, retry); err != nil {
		log.Print("")
		log.Fatal(err)
	}
	if !dry {
		os.RemoveAll(retry)
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

type dag struct {
	Name  string  `xml:"name,attr"`
	Tasks []*task `xml:"task"`
}

// writeDot ...
func (d *dag) writeDot(out io.Writer) error {
	w := bufio.NewWriter(out)
	if _, err := fmt.Fprintf(w, "digraph %s {\n", d.Name); err != nil {
		return err
	}
	if _, err := fmt.Fprint(w, "\n"); err != nil {
		return err
	}
	for _, t := range d.Tasks {
		if _, err := fmt.Fprintf(w, "  %s [label=\"%s\"];\n", t.Name, t.Name); err != nil {
			return err
		}
		for _, dep := range t.Deps {
			if _, err := fmt.Fprintf(w, "  %s -> %s;\n", dep, t.Name); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprint(w, "\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprint(w, "}\n"); err != nil {
		return err
	}
	return w.Flush()
}

// validate ...
func (d *dag) validate() error {
	if err := d.validateName(); err != nil {
		return err
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' name", d.Name)
	if err := d.validateTaskCount(); err != nil {
		return err
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' task count", d.Name)
	if err := d.validateTaskNames(); err != nil {
		return err
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' task names", d.Name)
	for _, t := range d.Tasks {
		if err := t.validateName(); err != nil {
			return err
		}
		log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' name", t.Name)
		if err := t.validateCmd(); err != nil {
			return err
		}
		log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' command", t.Name)
		if err := t.validateFiles(); err != nil {
			return err
		}
		for _, f := range t.Files {
			log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' file '%s'", t.Name, f)
		}
		if err := d.validateTaskDeps(t); err != nil {
			return err
		}
		for _, dep := range t.Deps {
			log.Printf("[\u001b[32mPASS\u001b[0m] check task '%s' dependency '%s'", t.Name, dep)
		}
	}
	if err := d.validateNoCircularDependency(); err != nil {
		return err
	}
	log.Printf("[\u001b[32mPASS\u001b[0m] check dag '%s' for circular dependency", d.Name)
	return nil
}

// validateName ...
func (d *dag) validateName() error {
	if d.Name == "" {
		return errors.New("[\u001b[31mFAIL\u001b[0m] dag name attribute must be set")
	}
	return nil
}

// validateTaskCount ...
func (d *dag) validateTaskCount() error {
	if len(d.Tasks) == 0 {
		return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] dag '%s' has no tasks", d.Name)
	}
	return nil
}

// validateTaskNames ...
func (d *dag) validateTaskNames() error {
	taskNames := map[string]struct{}{}
	for _, t := range d.Tasks {
		if _, exists := taskNames[t.Name]; exists {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] two tasks have the same name '%s'", t.Name)
		}
		taskNames[t.Name] = struct{}{}
	}
	return nil
}

// validateDeps ...
func (d *dag) validateTaskDeps(t *task) error {
	tasks := map[string]*task{}
	for _, t2 := range d.Tasks {
		tasks[t2.Name] = t2
	}
	for _, dep := range t.Deps {
		t2, ok := tasks[dep]
		if !ok {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' depends on missing task '%s'", t.Name, dep)
		}
		if t == t2 {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' depends on itself", t.Name)
		}
	}
	return nil
}

// circularDependecyCheck ...
// Kahn's algorithm
func (d *dag) validateNoCircularDependency() error {
	edges := []*edge{}
	for _, t := range d.Tasks {
		edges = append(edges, t.parentEdges()...)
	}
	s := []*task{}
	for _, t := range d.Tasks {
		if len(t.parents) == 0 {
			s = append(s, t)
		}
	}
	for len(s) > 0 {
		n := s[len(s)-1]
		s = s[0 : len(s)-1]
		next := []*edge{}
		for _, e := range edges {
			if e.parent == n {
				otherIncoming := false
				for _, e2 := range edges {
					if e != e2 && e.child == e2.child {
						otherIncoming = true
						break
					}
				}
				if !otherIncoming {
					s = append(s, e.child)
				}
			} else {
				next = append(next, e)
			}
		}
		edges = next
	}
	if len(edges) > 0 {
		path := ""
		for _, edge := range edges {
			path = fmt.Sprintf("%s, (%s) -> (%s)", path, edge.child.Name, edge.parent.Name)
		}
		return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] detected circular dependency %s", path)
	}
	return nil
}

// run ...
func (d *dag) run(workers int, retry string) error {
	var wg sync.WaitGroup
	wg.Add(len(d.Tasks))
	queue := make(chan *task, len(d.Tasks))
	for _, t := range d.Tasks {
		if t.isReady() {
			queue <- t
		}
	}
	var lock sync.Mutex
	for i := 0; i < workers; i++ {
		go worker(&wg, &lock, queue)
	}
	wg.Wait()
	errors := []error{}
	for _, t := range d.Tasks {
		if t.isReady() && !t.Success {
			errors = append(errors, fmt.Errorf("[ \u001b[31mERROR\u001b[0m ] %s", t.Name))
		}
	}
	if len(errors) > 0 {
		for _, err := range errors {
			log.Print(err)
		}
		if err := d.writeRetry(retry); err != nil {
			return err
		}
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		return fmt.Errorf("Failure (resumable dag stored in %s)", path.Join(wd, retry))
	}
	return nil
}

// writeRetry ...
func (d *dag) writeRetry(retry string) error {
	f, err := os.Create(retry)
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
	return f.Close()
}

// worker ...
func worker(wg *sync.WaitGroup, lock *sync.Mutex, queue chan *task) {
	for true {
		t := <-queue
		var err error
		if t.skip {
			log.Printf("[\u001b[34mSKIPPED\u001b[0m] %s (%s)", t.Name, t.command())
		} else {
			log.Printf("[\u001b[33mRUNNING\u001b[0m] %s (%s)", t.Name, t.command())
			if err = t.exec(); err == nil {
				log.Printf("[\u001b[32mSUCCESS\u001b[0m] %s", t.Name)
			}
		}
		lock.Lock()
		if err == nil {
			t.Success = true
			for _, child := range t.children {
				if child.isReady() {
					queue <- child
				}
			}
		} else {
			for range t.allChildren() {
				wg.Done()
			}
		}
		wg.Done()
		lock.Unlock()
	}
}

// newDag ...
func newDag(in io.Reader) (*dag, error) {
	r := bufio.NewReader(in)
	dec := xml.NewDecoder(r)
	d := dag{}
	if err := dec.Decode(&d); err != nil {
		return nil, err
	}
	for _, t1 := range d.Tasks {
		t1.skip = false
		if t1.Success {
			t1.skip = true
		}
		t1.Success = false
		t1.children = []*task{}
		t1.parents = []*task{}
		for _, t2 := range d.Tasks {
			for _, dep := range t1.Deps {
				if dep == t2.Name {
					t1.parents = append(t1.parents, t2)
				}
			}
			for _, dep := range t2.Deps {
				if dep == t1.Name {
					t1.children = append(t1.children, t2)
				}
			}
		}
	}
	return &d, nil
}

// task ...
type task struct {
	Cmd      string   `xml:"cmd"`
	Deps     []string `xml:"dep"`
	Files    []string `xml:"file"`
	Name     string   `xml:"name,attr"`
	Success  bool     `xml:"success"`
	skip     bool
	children []*task
	parents  []*task
}

// validateName ...
func (t *task) validateName() error {
	if t.Name == "" {
		return errors.New("[\u001b[31mFAIL\u001b[0m] task name attribute must be set")
	}
	return nil
}

// validateCmd ...
func (t *task) validateCmd() error {
	if t.Cmd == "" {
		return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' has no command set", t.Name)
	}
	return nil

}

// validateFiles ...
func (t *task) validateFiles() error {
	for _, f := range t.Files {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			return fmt.Errorf("[\u001b[31mFAIL\u001b[0m] task '%s' requires missing file '%s'", t.Name, f)
		}
	}
	return nil
}

// parentEdges ...
func (t *task) parentEdges() []*edge {
	edges := make([]*edge, len(t.parents))
	for i, parent := range t.parents {
		edges[i] = &edge{t, parent}
	}
	return edges
}

func (t *task) isReady() bool {
	for _, parent := range t.parents {
		if !parent.Success {
			return false
		}
	}
	return true
}

func (t *task) allChildren() []*task {
	set := map[string]*task{}
	for _, child := range t.children {
		set[child.Name] = child
		for _, grandChild := range child.allChildren() {
			set[grandChild.Name] = grandChild
		}
	}
	children := []*task{}
	for _, child := range set {
		children = append(children, child)
	}
	return children
}

func (t *task) command() string {
	files := make([]interface{}, len(t.Files))
	for i, f := range t.Files {
		files[i] = f
	}
	return fmt.Sprintf(t.Cmd, files...)
}

// exec ...
func (t *task) exec() error {
	args := strings.Fields(t.command())
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(os.Environ())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	errc := make(chan error)
	go func() {
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			log.Printf("%s (stderr) %s", t.Name, s.Text())
		}
		errc <- s.Err()
	}()
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			log.Printf("%s (stdout) %s", t.Name, s.Text())
		}
		errc <- s.Err()
	}()
	if err := cmd.Start(); err != nil {
		return err
	}
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			return err
		}
	}
	return cmd.Wait()
}

// edge ...
type edge struct {
	child  *task
	parent *task
}
