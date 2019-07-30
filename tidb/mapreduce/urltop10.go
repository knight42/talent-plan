package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type FixedSizeHeap struct {
	buf      []*urlCount
	size     int
	capacity int
}

func (pq *FixedSizeHeap) Pop() *urlCount {
	if pq.size == 0 {
		return nil
	} else if pq.size == 1 {
		pq.size = 0
		return pq.buf[0]
	}

	res := pq.buf[0]
	pq.size -= 1
	size := pq.size
	last := pq.buf[size]

	i := 1
	var child int
	for i*2 <= size {
		child = i * 2
		if child+1 <= size && pq.buf[child-1].cnt < pq.buf[child].cnt {
			child += 1
		}
		if last.cnt > pq.buf[child-1].cnt {
			break
		}
		pq.buf[i-1] = pq.buf[child-1]
		i = child
	}
	pq.buf[i-1] = last

	return res
}

func (pq *FixedSizeHeap) Empty() bool {
	return pq.size == 0
}

func (pq *FixedSizeHeap) Push(item *urlCount) {
	size := pq.size
	if size == 0 {
		pq.buf[0] = item
		pq.size += 1
		return
	}

	var hole int
	if size == pq.capacity {
		i := size
		minEle := pq.buf[i-1].cnt
		minEleIdx := i - 1
		i -= 1
		for i*2 > pq.size {
			if pq.buf[i-1].cnt < minEle {
				minEle = pq.buf[i-1].cnt
				minEleIdx = i - 1
			}
			i -= 1
		}
		if item.cnt <= minEle {
			return
		}
		hole = minEleIdx
	} else {
		hole = size
		pq.size += 1
	}
	parent := (hole + 1) / 2

	for parent > 0 {
		if item.cnt < pq.buf[parent-1].cnt {
			break
		}
		pq.buf[hole] = pq.buf[parent-1]
		hole = parent - 1
		parent /= 2
	}
	pq.buf[hole] = item

}

func NewFixedSizeHeap(n int) FixedSizeHeap {
	l := make([]*urlCount, n)
	return FixedSizeHeap{
		buf: l, size: 0, capacity: n,
	}
}

func HeapTopN(urlCntMap map[string]int, n int) ([]string, []int) {
	pq := NewFixedSizeHeap(n)
	for k, v := range urlCntMap {
		pq.Push(&urlCount{k, v})
	}
	urls := make([]string, 0, n)
	cnts := make([]int, 0, n)
	for !pq.Empty() {
		v := pq.Pop()
		urls = append(urls, v.url)
		cnts = append(cnts, v.cnt)
	}
	return urls, cnts
}

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	cntMap := map[string]int{}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		cntMap[l] += 1
	}
	kvs := make([]KeyValue, 0, len(cntMap))
	for k, v := range cntMap {
		kvs = append(kvs, KeyValue{Key: k, Value: fmt.Sprint(v)})
	}
	return kvs
}

// URLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	result := 0
	for _, v := range values {
		n, _ := strconv.Atoi(v)
		result += n
	}
	return fmt.Sprintf("%s %d\n", key, result)
}

// URLTop10Map is the map function in the first round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		kvs = append(kvs, KeyValue{"", l})
	}
	return kvs
}

// URLTop10Reduce is the reduce function in the second reound
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := HeapTopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
