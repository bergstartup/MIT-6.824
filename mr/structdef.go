package mr

import "time"

//Master decl
type Master struct {
	knownWorker     map[int]*worker
	jobQueue        map[int]*job
	taskAssignQueue []*task
	taskAssigned    map[int]*task
}

type worker struct {
	id             int
	isTaskAssigned bool
	taskAssigned   *task
}

type job struct {
	jobID             int
	mapTask           []*task
	noMapCompleted    int
	isMapCompleted    bool
	reduceTask        []*task
	noReduceCompleted int
	isReduceCompleted bool
}

type task struct {
	taskID       int
	ismapTask    bool
	inputFiles   []string
	isAssigned   bool
	timeAssigned time.Time
	assignedToID int
	isCompleted  bool
	jobPointer   *job
}
