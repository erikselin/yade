package main

import (
	"bytes"
	"testing"
)

var input = `
<dag name="test_dag">
  <task name="test_task_1">
    <file>test_task_1.py</file>
    <cmd>python %s</cmd>
  </task>
  <task name="test_task_2">
    <file>test_task_2.py</file>
    <cmd>python %s</cmd>
  </task>
  <task name="test_task_3">
    <dep>test_task_1</dep>
    <dep>test_task_2</dep>
    <file>test_task_3.py</file>
    <cmd>python %s</cmd>
  </task>
  <task name="test_task_4">
    <dep>test_task_3</dep>
    <file>test_task_4.py</file>
    <cmd>python %s</cmd>
  </task>
  <task name="test_task_5">
    <dep>test_task_1</dep>
    <dep>test_task_3</dep>
    <file>test_task_5.py</file>
    <cmd>python %s</cmd>
  </task>
</dag>
`

var expected = &dag{
	Name: "test_dag",
	Tasks: []*task{
		&task{
			Cmd:   "python %s",
			Deps:  []string{},
			Files: []string{"test_task_1.py"},
			Name:  "test_task_1",
		},
		&task{
			Cmd:   "python %s",
			Deps:  []string{},
			Files: []string{"test_task_2.py"},
			Name:  "test_task_2",
		},
		&task{
			Cmd:   "python %s",
			Deps:  []string{"test_task_1", "test_task_2"},
			Files: []string{"test_task_3.py"},
			Name:  "test_task_3",
		},
		&task{
			Cmd:   "python %s",
			Deps:  []string{"test_task_3"},
			Files: []string{"test_task_4.py"},
			Name:  "test_task_4",
		},
		&task{
			Cmd:   "python %s",
			Deps:  []string{"test_task_1", "test_task_3"},
			Files: []string{"test_task_5.py"},
			Name:  "test_task_5",
		},
	},
}

var expectedDot = `digraph test_dag {

  test_task_1 [label="test_task_1"];
  test_task_2 [label="test_task_2"];
  test_task_3 [label="test_task_3"];
  test_task_1 -> test_task_3;
  test_task_2 -> test_task_3;
  test_task_4 [label="test_task_4"];
  test_task_3 -> test_task_4;
  test_task_5 [label="test_task_5"];
  test_task_1 -> test_task_5;
  test_task_3 -> test_task_5;

}
`

func TestParseDag(t *testing.T) {
	r := bytes.NewBufferString(input)
	d, err := newDag(r)
	if err != nil {
		t.Errorf("newDag(r) returned error %v, want no error", err)
	}
	if d.Name != expected.Name {
		t.Errorf("newDag(r) returned dag where Name='%s', wanted Name='%s'", d.Name, expected.Name)
	}
	if len(d.Tasks) != len(expected.Tasks) {
		t.Errorf("newDag(r) returned dag where len(Tasks)='%d', wanted len(Tasks)='%d'", len(d.Tasks), len(expected.Tasks))
	}
	for i, et := range expected.Tasks {
		if et.Cmd != d.Tasks[i].Cmd {
			t.Errorf("newDag(r) returned dag where Tasks[%d].Cmd='%s', wanted Cmd='%s'", i, d.Tasks[i].Cmd, et.Cmd)
		}
		if len(et.Deps) != len(d.Tasks[i].Deps) {
			t.Errorf("newDag(r) returned dag where len(Tasks[%d].Deps)='%d', wanted len(Deps)='%d'", i, len(d.Tasks[i].Deps), len(et.Deps))
		} else {
			for j, ed := range et.Deps {
				if ed != d.Tasks[i].Deps[j] {
					t.Errorf("newDag(r) returned dag where Tasks[%d].Deps[%d]='%s', wanted Deps[%d]='%s'", i, j, d.Tasks[i].Deps[j], j, ed)
				}
			}
		}
		if len(et.Files) != len(d.Tasks[i].Files) {
			t.Errorf("newDag(r) returned dag where len(Tasks[%d].Files)='%d', wanted len(Files)='%d'", i, len(d.Tasks[i].Files), len(et.Files))
		} else {
			for j, ef := range et.Files {
				if ef != d.Tasks[i].Files[j] {
					t.Errorf("newDag(r) returned dag where Tasks[%d].Files[%d]='%s', wanted Files[%d]='%s'", i, j, d.Tasks[i].Files[j], j, ef)
				}
			}
		}
		if et.Name != d.Tasks[i].Name {
			t.Errorf("newDag(r) returned dag where Tasks[%d].Name='%s', wanted Name='%s'", i, d.Tasks[i].Name, et.Name)
		}
	}
}

func TestWriteDot(t *testing.T) {
	var w bytes.Buffer
	if err := expected.writeDot(&w); err != nil {
		t.Errorf("writeDot(w) returned error %v, want no error", err)
	}
	if expectedDot != w.String() {
		t.Errorf("writeDot(w) wrote '%s' to w, wanted '%s'", w.String(), expectedDot)
	}
}

func TestValidateName(t *testing.T) {
	d := &dag{}
	if err := d.validateName(); err == nil {
		t.Errorf("validateName() returned nil, want error")
	}
	if err := expected.validateName(); err != nil {
		t.Errorf("validateName() returned error %v, want no error", err)
	}
}

func TestValidateTaskCount(t *testing.T) {
	d := &dag{}
	if err := d.validateTaskCount(); err == nil {
		t.Errorf("validateTaskCount() returned nil, want error")
	}
	if err := expected.validateTaskCount(); err != nil {
		t.Errorf("validateTaskCount() returned error %v, want no error", err)
	}
}

func TestValidateTaskNames(t *testing.T) {
	d := &dag{
		Tasks: []*task{
			&task{
				Name: "task1",
			},
			&task{
				Name: "task1",
			},
		},
	}
	if err := d.validateTaskNames(); err == nil {
		t.Errorf("validateTaskNames() returned nil, want error")
	}
	if err := expected.validateTaskNames(); err != nil {
		t.Errorf("validateTaskNames() returned error %v, want no error", err)
	}
}

func TestValidateTaskDeps(t *testing.T) {
	d := &dag{
		Tasks: []*task{
			&task{
				Deps: []string{"missing_task"},
			},
		},
	}
	if err := d.validateTaskDeps(d.Tasks[0]); err == nil {
		t.Errorf("validateTaskDeps() returned nil, want error")
	}
	if err := expected.validateTaskDeps(expected.Tasks[3]); err != nil {
		t.Errorf("validateTaskDeps() returned error %v, want no error", err)
	}
}

func TestValidateNoCircularDependency(t *testing.T) {
	task1 := &task{
		Name: "task1",
		Deps: []string{"task2"},
	}
	task2 := &task{
		Name: "task2",
		Deps: []string{"task1"},
	}
	task1.parents = []*task{task2}
	task1.children = []*task{task2}
	task2.parents = []*task{task1}
	task2.children = []*task{task1}
	d := &dag{
		Tasks: []*task{task1, task2},
	}
	if err := d.validateNoCircularDependency(); err == nil {
		t.Errorf("validateNoCircularDependency() returned nil, want error")
	}
	if err := expected.validateNoCircularDependency(); err != nil {
		t.Errorf("validateNoCircularDependency() returned error %v, want no error", err)
	}
}

func TestTaskValidateName(t *testing.T) {
	s := &task{}
	if err := s.validateName(); err == nil {
		t.Errorf("validateName() returned nil, want error")
	}
	if err := expected.Tasks[0].validateName(); err != nil {
		t.Errorf("validateName() returned error %v, want no error", err)
	}
}

func TestTaskValidateCmd(t *testing.T) {
	s := &task{}
	if err := s.validateCmd(); err == nil {
		t.Errorf("validateCmd() returned nil, want error")
	}
	if err := expected.Tasks[0].validateCmd(); err != nil {
		t.Errorf("validateCmd() returned error %v, want no error", err)
	}
}
