package mr

// RPC相关定义

import "os"
import "strconv"


// 实例了如何定义RPC的arguments和reply
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 你的rpc相关定义


// 生成一个unix-domain socket name
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}