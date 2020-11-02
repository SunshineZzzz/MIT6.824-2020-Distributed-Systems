package main

// 文本索引器，单词所在文本名称
// go build -buildmode=plugin indexer.go

import "fmt"
import "../mr"
import "strings"
import "unicode"
import "sort"

func Map(document string, value string) (res []mr.KeyValue) {
	m := make(map[string]bool)
	words := strings.FieldsFunc(value, func(x rune) bool { return !unicode.IsLetter(x) })
	for _, w := range words {
		m[w] = true
	}
	for w := range m {
		kv := mr.KeyValue{w, document}
		res = append(res, kv)
	}
	return
}

func Reduce(key string, values []string) string {
	sort.Strings(values)
	return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
}