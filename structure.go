package memstore

import (
	"github.com/mngharbi/GoLLRB/llrb"
	"sync"
)

/*
	Interface for any element

	Includes comparator for every index
*/
type Item interface {
	Less(index string, than interface{}) bool
}

type internalItem struct {
	item *Item
}

func (ii *internalItem) Less(index string, than llrb.Item) bool {
	ithan := than.(*internalItem)
	var anonItem interface{} = *(ithan.item)
	var item Item = *(ii.item)
	return item.Less(index, anonItem)
}

/*
	Memstore object
	Nothing is exported
*/
type Memstore struct {
	// Slice of trees for each index
	trees []*llrb.LLRB

	// Map of indexes we're supporting
	indexTree map[string]*llrb.LLRB

	// RW lock
	m sync.RWMutex
}
