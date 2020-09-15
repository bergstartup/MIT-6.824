package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//KeyValue
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

type workerEventQ struct {
	taskQueue []*workerTask
	trigger   chan int
	id        int
	heartbeat int
	mapf      func(string, string) []KeyValue
	reducef   func(string, []string) string
	nTask     int
}

type workerTask struct {
	taskID  int
	isMap   bool
	nReduce int
	url     []string
}

/*
Own function(s)
*/
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *workerEventQ) introduceToMaster() {
	args := Inputargs{}
	reply := Exportvalues{}
	//Set unique id

	args.New = true
	args.Heartbt = 1
	call("Master.Update", &args, &reply)
	//If recv
	//fmt.Println(reply)
	w.heartbeat = 1
	w.nTask = 0
	w.id = reply.ID
	if reply.WorkAssigned {
		w.taskQueue = append(w.taskQueue, &workerTask{isMap: reply.IsMap, nReduce: reply.NReduced, url: reply.URL, taskID: reply.TaskId})
		w.trigger <- 1
	}
	//fmt.Println("My id is : ", reply.ID)
}

func (w *workerEventQ) completed(files map[int]string) {
	args := Inputargs{}
	reply := Exportvalues{}
	//Set unique id

	args.New = false
	args.ResultFile = files
	args.Completed = true
	args.ID = w.id
	args.Heartbt = w.heartbeat
	//fmt.Println(args)
	call("Master.Update", &args, &reply)
	//If recv
	//fmt.Println(reply)
	if reply.WorkAssigned {
		w.taskQueue = append(w.taskQueue, &workerTask{isMap: reply.IsMap, nReduce: reply.NReduced, url: reply.URL, taskID: reply.TaskId})
		w.trigger <- 1
	}
}

func (w *workerEventQ) execute(thread *workerTask) {
	w.nTask++
	//For artifical failures
	//time.Sleep(2 * time.Second)
	//if rand.Intn(100) < 50 {
	//fmt.Println("Abondend", thread.taskID)
	//return
	//}

	if thread.isMap {
		//map
		file, _ := os.Open(thread.url[0])
		content, _ := ioutil.ReadAll(file)
		file.Close()
		kva := w.mapf(thread.url[0], string(content))
		//split in accordance to key of the values
		var hashPair map[int][]KeyValue
		hashPair = make(map[int][]KeyValue, 1)
		for _, kv := range kva {
			hashPair[ihash(kv.Key)%thread.nReduce] = append(hashPair[ihash(kv.Key)%thread.nReduce], kv)
		}
		//free kva
		kva = nil
		//Store to files according to hash%nreduce
		var fileURL map[int]string
		fileURL = make(map[int]string, 1)
		baseFileName := "tmp" + strconv.Itoa(w.nTask) + "-" + strconv.Itoa(w.id) + "-"
		for index, hash := range hashPair {
			//Write to json file
			fileName := baseFileName + strconv.Itoa(index)
			fileURL[index] = fileName
			file, _ := os.Create(fileName)
			enc := json.NewEncoder(file)

			for _, kv := range hash {
				_ = enc.Encode(&kv)
			}
			file.Close()
		}
		//fmt.Println(fileURL)

		//Send completed msg
		w.completed(fileURL)

	} else {
		//reduce job
		kva := []KeyValue{}
		for _, fileName := range thread.url {
			file, _ := os.Open(fileName)
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
		//copied from wcsequential.go
		sort.Sort(ByKey(kva))
		oname := "mr-out-" + strconv.Itoa(thread.taskID-1)
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := w.reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		ofile.Close()
		files := make(map[int]string, 1)
		files[0] = oname
		w.completed(files)
	}
}

func (w *workerEventQ) checkForTask() {
	for {
		select {
		case <-w.trigger:
			//fmt.Println(w.id)
			//fmt.Println("Executing :", w.taskQueue[0].taskID)
			go w.execute(w.taskQueue[0])
			w.taskQueue = w.taskQueue[1:]
		}
	}
}

func (w *workerEventQ) sendHeartBeat() {
	c := time.Tick(5 * time.Second)
	for _ = range c {
		w.heartbeat++
		args := Inputargs{}
		reply := Exportvalues{}
		//Set unique id

		args.New = false
		args.Completed = false
		args.ID = w.id
		args.Heartbt = w.heartbeat
		call("Master.Update", &args, &reply)
		//If recv
		//fmt.Println(reply)
		if reply.WorkAssigned {
			w.taskQueue = append(w.taskQueue, &workerTask{isMap: reply.IsMap, nReduce: reply.NReduced, url: reply.URL, taskID: reply.TaskId})
			w.trigger <- 1
		}
	}
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//Worker called by mrworker.go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := workerEventQ{}
	w.mapf = mapf
	w.reducef = reducef
	w.trigger = make(chan int)
	go w.checkForTask()
	w.introduceToMaster()
	w.sendHeartBeat()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
