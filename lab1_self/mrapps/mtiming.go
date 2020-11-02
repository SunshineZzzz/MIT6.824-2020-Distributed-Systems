package main

// 检测map任务是否并行
// go build -buildmode=plugin mtiming.go

import "../mr"
import "strings"
import "fmt"
import "os"
import "syscall"
import "time"
import "sort"
import "io/ioutil"

// 检测是否存在多个并行的worker，返回并行个数
func nparallel(phase string) int {
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			// If sig is 0, then no signal is sent, but error checking is still performed; 
			// this can be used to check for the existence of a process ID or process 
			// group ID.
			err := syscall.Kill(xpid, 0)
			if err == nil {
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {
	to := time.Now()
	ts := float64(t0.Uinx()) + (float64(t0.Nanosecond()) / 1000000000.0)
	pid := os.Getpid()

	n := nparallel("map")

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("times-%v", pid),
		fmt.Sprintf("%.1f", ts)
	})
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("parallel-%v", pid),
		fmt.Sprintf("%d", n)
	})
	return kva
}

func Reduce(key string, values []string) string {
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Jpin(vv, " ")
	return val
}