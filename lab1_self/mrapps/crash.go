package main

// MapReduce程序，有时候会崩溃，有时候会耗费很长时间
// 目的是检测MapReduce分布式系统的恢复能力
// go build -buildmode=plugin crash.go

import "../mr"
import crand "crypto/rand"
import "math/big"
import "strings"
import "os"
import "sort"
import "strconv"
import "time"

func maybeCrash() {
	max := big.NewInt(1000)
	// [0,max)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 {
		// 模拟崩溃
		os.Exit(1)
	} else if rr.Int64() < 660 {
		//休息一会
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
}

func Map(filename string, contents string) []mr.KeyValue {
	maybeCrash()

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{"a", filename})
	kva = append(kva, mr.KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, mr.KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, mr.KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}