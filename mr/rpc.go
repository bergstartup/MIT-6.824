package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

//Inputargs : arg for Worker->Master call [1]
type Inputargs struct {
	New        bool
	Heartbt    int
	ID         int
	Completed  bool
	ResultFile map[int]string
}

//Exportvalues decl
type Exportvalues struct {
	ID           int
	WorkAssigned bool
	IsMap        bool
	NReduced     int
	TaskId       int
	URL          []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
