package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type DirectBlock struct {
	sa [65536]string
	m  *sync.RWMutex
}

type DirectCache struct {
	blocks    []*DirectBlock
	blocksnum uint16
	stop      int32
}

func NewDirectCache(blocksnum uint16, evictFunc func(string) bool) (dc *DirectCache) {
	blocks := make([]*DirectBlock, blocksnum)
	dc = &DirectCache{
		blocksnum: blocksnum,
		blocks:    blocks,
	}
	for i := uint16(0); i < blocksnum; i++ {
		dc.blocks[i] = &DirectBlock{
			m: &sync.RWMutex{},
		}
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Fatalln("evict error:", err)
			}
			for atomic.LoadInt32(&dc.stop) == 0 {
				time.Sleep(1 * time.Second)
				for _, b := range dc.blocks {
					time.Sleep(1 * time.Second)
					b.m.Lock()
					for i, s := range b.sa {
						var evict bool = false
						if evict = evictFunc(s); evict {
							b.sa[i] = ""
						}
					}
					b.m.Unlock()
				}
			}
		}()
	}()
	runtime.SetFinalizer(dc, func(dc *DirectCache) {
		atomic.StoreInt32(&dc.stop, 1)
	})
	return dc
}

func hash(s string) (h uint16) {
	ha := 0
	for _, b := range []byte(s) {
		ha += int(b)
		ha %= 65537
	}
	return uint16(ha%65535) + 1
}

func (dc *DirectCache) Add(s string) {
	h := hash(s)
	for _, b := range dc.blocks {
		b.m.Lock()
		if b.sa[h] == "" {
			b.sa[h] = s
			b.m.Unlock()
			return
		}
		b.m.Unlock()
	}
	b := dc.blocks[h%dc.blocksnum]
	b.m.Lock()
	b.sa[h] = s
	b.m.Unlock()
	return
}

func (dc *DirectCache) Exist(s string) (ret bool) {
	h := hash(s)
	for _, b := range dc.blocks {
		b.m.RLock()
		if b.sa[h] == s {
			b.m.RUnlock()
			return true
		}
		b.m.RUnlock()
	}
	return false
}

func main() {
	dc := NewDirectCache(8, func(s string) bool {
		return false
	})
	dc.Add("你好")
	dc.Add("色情")
	dc.Add("政治")
	fmt.Println(dc.Exist("大家好"))
	fmt.Println(dc.Exist("色情"))

	return
}
