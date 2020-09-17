package main

// 统计单词个数的插件
// go build -buildmode=plugin wc.go

import "../mr"
import "unicode"
import "strings"
import "strconv"

// Mapc处理函数
// filename 文件名称
// contents 文件内容
func Map(filename string, contents string) []mr.KeyValue {
	// 分割单词
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// 分割contents，返回单词切片
	words := strings.FieldsFunc(contents, ff);

	// intermediate date
	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

// Reduce函数
// key 单词
// values 次数切片
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}