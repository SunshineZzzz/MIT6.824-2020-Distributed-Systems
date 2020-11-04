package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "sync"
import "time"
import "strconv"

// 最大允许执行时间
const MaxTaskRunTimeDuration = time.Second * 10

// Master 
type Master struct {
	// 你的定义
	// 任务池
	TaskPoolMap map[uint64]Task
	// 执行中任务池 index<->task
	RunningPoolMap map[uint64]Task
	// 完成任务池 index<->task
	FinishPoolMap map[uint64]Task
	// 完成任务的下标切片
	MapIndex []uint64
	// 输入文件名称
	Files []string
	// 阶段map/reduce
	Phase TaskPhase
	// 是否完成任务
	IsDone bool
	// reduce数目
	ReduceNum int
	// map文件的个数
	MapFileNum int
	// 生成workerid
	genWorkerId uint64
	// 自增任务id
	genTaskId uint64
	// 互斥量
	mutex sync.Mutex
}

// 初始化reduce任务
func (m *Master) initReduceTask() {
	// 这里不加锁了，调用这个函数前应该加锁了，TODO加个断言判断是否已经加锁
	if len(m.TaskPoolMap) != 0 {
		log.Fatalf("len m.TaskPoolMap %d not equal 0 \n", len(m.TaskPoolMap))
	}
	if len(m.RunningPoolMap) != 0 {
		log.Fatalf("len m.RunningPoolMap %d not equal 0 \n", len(m.RunningPoolMap))
	}
	if len(m.FinishPoolMap) != m.MapFileNum {
		log.Fatalf("len m.FinishPoolMap %d not equal m.MapFileNum %d \n", len(m.FinishPoolMap), m.MapFileNum)
	}
	indexSlice := make([]uint64, m.MapFileNum)
	m.MapIndex = make([]uint64, m.MapFileNum)
	for index, task := range m.FinishPoolMap {
		task.Phase = TaskPhaseReduce
		task.Index = index
		task.Status = TaskStatusPool
		task.StartRunTime = time.Unix(0, 0)
		task.FileName = ""

		m.MapIndex = append(m.MapIndex, index)
		m.TaskPoolMap[index] = task

		indexSlice = append(indexSlice, index)
	}
	m.FinishPoolMap = make(map[uint64]Task, m.MapFileNum)
	for index, task := range m.TaskPoolMap {
		task.MapIndex = append(task.MapIndex, indexSlice...)
		m.TaskPoolMap[index] = task
	}
}

// 检测超时任务，并且将超时的任务放回池子中
func (m *Master) checkTimeoutTask() {
	var tmpTask []Task
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for index, task := range m.RunningPoolMap {
		now := time.Now()
		curTimeDuration := now.Sub(task.StartRunTime)
		if curTimeDuration > MaxTaskRunTimeDuration {
			log.Printf("now: %v, startruntime: %v, diff: %v\n", 
				now.Unix(), task.StartRunTime.Unix(), (now.Unix() - task.StartRunTime.Unix()))
			tmpTask = append(tmpTask, task)
			delete(m.RunningPoolMap, index)
		}
	}
	for _, task := range tmpTask {
		m.genTaskId++
		task.Index = m.genTaskId
		task.Status = TaskStatusPool
		m.TaskPoolMap[task.Index] = task
	}
}

// 是否所有阶段任务都完成
func (m *Master) isAllPhaseDone() bool {
	m.checkTimeoutTask()
	bFinish := true
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// 判断是否完成工作
	for {
		if len(m.TaskPoolMap) > 0 {
			bFinish = false
			break
		}
		if len(m.RunningPoolMap) > 0 {
			bFinish = false
			break
		}
		if true {
			break
		}
	}
	// 任务完成
	if bFinish {
		if m.Phase == TaskPhaseMap {
			m.initReduceTask()
			m.IsDone = false
		} else {
			m.IsDone = true
		}
	} else {
		m.IsDone = false
	}

	return m.IsDone
}

// 你的代码 -- 提供给worker的RPC相关函数

// 一个RPC实例
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// hello rpc
func (m *Master) HelloRPC(args *HelloArgs, reply *HelloReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.genWorkerId++
	reply.Id = m.genWorkerId

	log.Printf("worker hello: %v\n", reply)
	return nil
}

// require task rpc
func (m *Master) RequireTaskRPC(args *ReqTaskArgs, reply *ReqTaskReply) error {
	reply.IsDone = false
	reply.PoolDone = false
	
	reply.IsDone = m.isAllPhaseDone()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.TaskPoolMap) > 0 {
		for index, task := range m.TaskPoolMap {
			task.Status = TaskStatusRunning
			task.StartRunTime = time.Now()
			m.RunningPoolMap[index] = task
			delete(m.TaskPoolMap, index)
			reply.Task = task
			break
		}
	} else {
		reply.PoolDone = true
	}
	
	log.Printf("%d worker request task: %v\n", args.Id, reply)
	return nil
}

// report task rpc
func (m *Master) ReportTaskRPC(args *RepTaskArgs, reply *RepTaskReply) error {
	var isAck bool = false
	var index uint64 = args.Index
	var newFileName string = ""
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args.IsDone {
		_, ok := m.RunningPoolMap[index]
		// 没有被移除，说明按时完成任务了
		if ok {
			isAck = true
			task, has := m.FinishPoolMap[index]
			if has {
				log.Fatalf("%d worker, m.FinishPoolMap[%d] already exists task: %v\n", args.Id, index, task)
			}
			task = m.RunningPoolMap[index]
			task.Status = TaskStatusFinish
			m.FinishPoolMap[index] = task
			delete(m.RunningPoolMap, index)
			if (m.Phase == TaskPhaseReduce && task.Phase == TaskPhaseReduce) {
				oldName := "reduce-tmp-mr-out-" + strconv.FormatUint(task.Index, 10)
				newFileName = "mr-out-" + strconv.Itoa(task.OriginIndex)
				os.Rename(oldName, newFileName)
			}
		}
	}
	reply.IsAck = isAck

	log.Printf("%d worker report task args: %v, reply: %v, newFileName: %s\n", args.Id, args, reply, newFileName)
	return nil
}

// 开启一个协程监听RPC为worker提供服务
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go 周期性的调用该函数，
// 返回所有worker job是否完成任务
func (m *Master) Done() bool {
	ret := false

	// 你的代码
	ret = m.isAllPhaseDone()
	return ret
}

// 创建一个Master
// main/mrmaster.go 调用该函数
// nReduce代表reduce task数量
func MakeMaster(files []string, nReduce int) *Master {
	log.SetFlags(log.Lshortfile | log.LUTC)

	m := Master{}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	// 你的代码
	mapFileNum := len(files)
	m.TaskPoolMap = make(map[uint64]Task, mapFileNum)
	m.RunningPoolMap = make(map[uint64]Task, mapFileNum)
	m.FinishPoolMap = make(map[uint64]Task, mapFileNum)
	m.MapIndex = make([]uint64, mapFileNum)
	m.Files = files
	m.Phase = TaskPhaseMap
	m.IsDone = false
	m.ReduceNum = nReduce
	m.MapFileNum = mapFileNum
	for i := 0; i < mapFileNum; i++ {
		m.TaskPoolMap[uint64(i)] = Task {
			Phase: TaskPhaseMap,
			Index: uint64(i),
			OriginIndex: i,
			Status: TaskStatusPool,
			FileName: files[i],
			ReduceNum: nReduce,
			MapIndex: make([]uint64, mapFileNum),
		}
		m.genTaskId = uint64(i)
	}
	m.genWorkerId = 0
	
	// 开启线程监听
	m.server()
	return &m
}