package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

func assignFilesReduce(jobPointer *job, files map[int]string) {
	for index, fileName := range files {
		jobPointer.reduceTask[index].inputFiles = append(jobPointer.reduceTask[index].inputFiles, fileName)
	}
}

func (m *Master) taskComplete(taskCompleted *task, file map[int]string) {
	//Delete from Assignedqueue
	if taskCompleted.isCompleted {
		return
	}
	//fmt.Println("Task completed", taskCompleted.taskID)
	taskCompleted.isCompleted = true
	delete(m.taskAssigned, taskCompleted.taskID)
	jobPointer := taskCompleted.jobPointer
	if taskCompleted.ismapTask {
		//If task in map
		jobPointer.noMapCompleted++
		if jobPointer.noMapCompleted == len(jobPointer.mapTask) {
			jobPointer.isMapCompleted = true
			//Add all reduce task for corresponding map
			for _, rtask := range jobPointer.reduceTask {
				m.taskAssignQueue = append(m.taskAssignQueue, rtask)
			}
		} //end of inner if
		assignFilesReduce(jobPointer, file)
	} else {
		//If task in reduce
		jobPointer.noReduceCompleted++
		if jobPointer.noReduceCompleted == len(jobPointer.reduceTask) {
			jobPointer.isReduceCompleted = true
		} //end of inner if
	} //end of else
} //end of func

func (m *Master) assignTask(workerID int) ([]string, *task) {
	if len(m.taskAssignQueue) == 0 {
		return nil, nil
	}
	taskAssign := m.taskAssignQueue[0]
	if taskAssign.isCompleted {
		//Dequeue
		m.taskAssignQueue = m.taskAssignQueue[1:]
		return m.assignTask(workerID)
	}
	//If file for a reduce task is nil, mark it as completed
	if taskAssign.inputFiles == nil {
		m.taskComplete(taskAssign, nil)
		m.taskAssignQueue = m.taskAssignQueue[1:]
		return m.assignTask(workerID)
	}
	taskAssign.isAssigned = true
	taskAssign.assignedToID = workerID
	taskAssign.timeAssigned = time.Now()
	files := taskAssign.inputFiles
	//Dequeue
	m.taskAssignQueue = m.taskAssignQueue[1:]
	m.taskAssigned[taskAssign.taskID] = taskAssign
	return files, taskAssign
}

//Update RPC called by worker during init, heartbeat and taskcompletion
func (m *Master) Update(args *Inputargs, reply *Exportvalues) error {
	//switch the call
	var workerNode *worker
	switch {

	//Worker is introducing to master
	case args.New:
		//Add new know Worker
		workerNode = &worker{id: len(m.knownWorker) + 1, isTaskAssigned: false}
		m.knownWorker[len(m.knownWorker)+1] = workerNode

	//If worker is initmating a task complete
	case args.Completed:
		//Set that task to complete
		workerNode = m.knownWorker[args.ID]
		//Check if that task was already completed
		if !workerNode.taskAssigned.isCompleted {
			m.taskComplete(workerNode.taskAssigned, args.ResultFile)
		}
		//Mark node as job unassigned
		workerNode.isTaskAssigned = false

	//If heartbeat
	default:
		workerNode = m.knownWorker[args.ID]
		if workerNode.isTaskAssigned {
			workerNode.isTaskAssigned = workerNode.taskAssigned.isAssigned
		}
		if workerNode.isTaskAssigned {
			reply.WorkAssigned = false
			return nil
		}
	}
	//Lock
	files, taskAssign := m.assignTask(workerNode.id)
	//Unlock
	if files == nil {
		reply.WorkAssigned = false
	} else {
		reply.WorkAssigned = true
		reply.URL = files
		reply.IsMap = taskAssign.ismapTask
		reply.NReduced = len(taskAssign.jobPointer.reduceTask)
		reply.TaskId = taskAssign.taskID
		workerNode.isTaskAssigned = true
		workerNode.taskAssigned = taskAssign
		//fmt.Println(reply.IsMap, taskAssign.taskID, " to ", workerNode.id)
	}
	reply.ID = workerNode.id
	//Debug the return value
	return nil
}

// server: start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) createJob(files []string, nReduce int) {
	m.jobQueue = make(map[int]*job, 1)
	newJob := &job{jobID: len(m.jobQueue) + 1}
	//Create map task
	var newTask *task
	for index, url := range files {
		newTask = &task{taskID: index + 1, ismapTask: true, jobPointer: newJob}
		newTask.inputFiles = append(newTask.inputFiles, url)
		newJob.mapTask = append(newJob.mapTask, newTask)
		m.taskAssignQueue = append(m.taskAssignQueue, newTask)
		fmt.Println(url)
	}
	//Create reduce task
	for i := 1; i <= nReduce; i++ {
		newTask = &task{taskID: i, ismapTask: false, jobPointer: newJob}
		newJob.reduceTask = append(newJob.reduceTask, newTask)
	}
	m.jobQueue[newJob.jobID] = newJob
}

func (m *Master) checkTaskExpiry() {
	c := time.Tick(5 * time.Second)
	for _ = range c {
		for index, task := range m.taskAssigned {
			if time.Now().Sub(task.timeAssigned) > 10*time.Second {
				//fmt.Println(index, "expired was assigned to ", task.assignedToID, " at ", task.timeAssigned, " has files ", task.inputFiles)
				task.isAssigned = false
				m.taskAssignQueue = append(m.taskAssignQueue, task)
				delete(m.taskAssigned, index)
			}
		}

	}
}

//MakeMaster is called by mrmaster.go, with files and no_of_reduce jobs as arguments
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.knownWorker = make(map[int]*worker, 1)
	m.taskAssigned = make(map[int]*task, 1)
	//Calling RPC listner for master struct
	m.server()
	//Create a job and its subtask
	m.createJob(files, nReduce)
	//Append map task to assign taskAssignQueue
	//fmt.Println("Ready and Loaded")
	//fmt.Println(m.taskAssignQueue)
	go m.checkTaskExpiry()
	return &m
}

//Done func : ret must set to true after job has completed
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	for index, job := range m.jobQueue {
		if job.isReduceCompleted {
			delete(m.jobQueue, index)
		}
	}
	if len(m.jobQueue) == 0 {
		ret = true
	}
	return ret
}
