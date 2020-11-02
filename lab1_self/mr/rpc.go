package mr

// RPC相关定义

import "os"
import "strconv"
import "time"

// 实例了如何定义RPC的arguments和reply
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 你的rpc相关定义

// 任务状态类型
type TaskStatus string
// 任务阶段类型
type TaskPhase string

// 任务状态
const (
	// 池子中
	TaskStatusPool TaskStatus = "pool"
	// 执行中
	TaskStatusRunning TaskStatus = "running"
	// 已完成
	TaskStatusFinish TaskStatus = "finish"
)

// 任务阶段
const (
	// map阶段
	TaskPhaseMap TaskPhase = "map"
	// reduce阶段
	TaskPhaseReduce TaskPhase = "reduce"
)

// 任务
type Task struct {
	// 阶段map/reduce
	Phase TaskPhase
	// 任务队列中的下标
	Index uint64
	// 最初的下标 0,1,2,...n
	// master初始化时赋值，主要用于worker reduce阶段
	// mr-mapIndex-OriginIndex
	OriginIndex uint64
	// 任务状态
	Status TaskStatus
	// 开始执行任务时间
	StartRunTime time.Time
	// 文件名
	FileName string
	// reduce个数
	ReduceNum int
	// map任务的下标
	MapIndex []uint64
}

// hello
type HelloArgs struct {}
type HelloReply struct {
	Id uint64
}

// task
type ReqTaskArgs struct {
	Id uint64
}
type ReqTaskReply struct {
	// 任务
	Task Task
	// 池子中的任务做完了
	PoolDone bool
	// 所有阶段任务都完成了
	IsDone bool
}

// report
type RepTaskArgs struct {
	Id uint64
	// 任务序号
	Index uint64
	// 是否完成
	IsDone bool
}
type RepTaskReply struct {
	// 是否响应
	IsAck bool
}

// 生成一个unix-domain socket name
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}