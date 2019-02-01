package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type dag struct {
	Name  string  `xml:"name,attr"`
	Tasks []*task `xml:"task"`
}

// writeDot ...
func (d *dag) writeDot(w io.Writer) error {
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
	return nil
}

// circularDependecyCheck ...
// Kahn's algorithm
func (d *dag) circularDependencyCheck() error {
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
		return fmt.Errorf("detected circular dependency %s", path)
	}
	return nil
}

// exec ...
func (d *dag) exec(workers int) {
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
		go func(lock *sync.Mutex, queue chan *task) {
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
					wg.Done()
				} else {
					for i := 0; i < t.countChildren()+1; i++ {
						wg.Done()
					}
				}
				lock.Unlock()
			}
		}(&lock, queue)
	}
	wg.Wait()
}

// listErrors ...
func (d *dag) listErrors() []error {
	errors := []error{}
	for _, t := range d.Tasks {
		if t.isReady() && !t.Success {
			errors = append(errors, fmt.Errorf("[ \u001b[31mERROR\u001b[0m ] %s", t.Name))
		}
	}
	return errors
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

func (t *task) countChildren() int {
	n := 0
	for _, child := range t.children {
		n = n + 1 + child.countChildren()
	}
	return n
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

// newDag ...
func newDag(filename string) (*dag, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	dec := xml.NewDecoder(f)
	d := dag{}
	if err = dec.Decode(&d); err != nil {
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
