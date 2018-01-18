package memstore

import (
	"github.com/mngharbi/GoLLRB/llrb"
)


// Make internal item (to work with llrb) from external item
func makeInternalItem(item Item) llrb.Item  {
	itemCopy := item
	return &internalItem{
		item: &itemCopy,
	}
}

// Delete item from a certain tree
func (ms *Memstore) delete (x *internalItem, tree *llrb.LLRB) *internalItem {
	deleted := tree.Delete(x)
	if deleted == nil {
		return nil
	}
	return deleted.(*internalItem)
}

/*
	All exported functions/methods
*/

func New(indexes []string) *Memstore {
	ms := &Memstore{}

	ms.indexTree = map[string]*llrb.LLRB{}

	// Create trees and reverse dictionary
	ms.trees = make([]*llrb.LLRB, len(indexes))
	for i, index := range indexes {
		ms.trees[i] = llrb.New(index)
		ms.indexTree[index] = ms.trees[i]
	}

	return ms
}

func (ms *Memstore) Add (x Item) {
	// Make internal node to use in llrb
	ix := makeInternalItem(x)

	ms.m.Lock()

	// Add to every internal tree
	for _, tree := range ms.indexTree {
		tree.ReplaceOrInsert(ix)
	}

	ms.m.Unlock()
}

func (ms *Memstore) Delete (x Item, index string) Item {
	// Make internal node to use in llrb
	ix := makeInternalItem(x)

	// Get corresponding tree
	initialTree := ms.indexTree[index]
	if initialTree == nil {
		return nil
	}

	ms.m.Lock()

	var res *Item = nil

	// Delete from corresponding internal tree
	initialDeleted := ms.delete(ix.(*internalItem), initialTree)
	if initialDeleted == nil {
		return nil
	}
	res = initialDeleted.item

	// Remove from other trees using full object
	for _, tree := range ms.indexTree {
		if tree == initialTree {
			continue
		}
		var internalItem llrb.Item = initialDeleted
		tree.Delete(internalItem)
	}

	ms.m.Unlock()

	return *res
}

func (ms *Memstore) Get(x Item, index string) (res Item) {
	// Make internal node to use with llrb
	ix := makeInternalItem(x)

	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	ms.m.RLock()

	ifound := tree.Get(ix)
	if ifound == nil {
		res = nil
	} else {
		res = *(ifound.(*internalItem).item)
	}

	ms.m.RUnlock()

	return res
}


func (ms *Memstore) GetRange (from, to Item, index string, test (func(Item) bool)) {
	// Make internal nodes to use with llrb
	ifrom := makeInternalItem(from)
	ito := makeInternalItem(to)

	// Transform iterator
	iterator := func(it llrb.Item) bool {
		extItPtr := it.(*internalItem).item
		return test(*extItPtr)
	}

	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return
	}

	ms.m.RLock()

	tree.AscendRange(ifrom.(llrb.Item), ito.(llrb.Item), iterator)

	ms.m.RUnlock()
}

func (ms *Memstore) Len() (res int) {
	// Get first tree
	tree := ms.trees[0]

	ms.m.RLock()

	// Look up size
	res = tree.Len()

	ms.m.RUnlock()

	return res
}

func (ms *Memstore) Max(index string) (res Item) {
	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	ms.m.RLock()

	// Look up max
	maxResult := tree.Max()
	if maxResult == nil {
		res = nil
	} else {
		res = *(maxResult.(*internalItem).item)
	}

	ms.m.RUnlock()

	return res
}

func (ms *Memstore) Min(index string) (res Item) {
	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	ms.m.RLock()

	// Look up min
	minResult := tree.Min()
	if minResult == nil {
		res = nil
	} else {
		res = *(minResult.(*internalItem).item)
	}

	ms.m.RUnlock()

	return res
}

func (ms *Memstore) UpdateData(x Item, index string, modify (func(interface{}) (interface{}, bool)) ) (res interface{}) {
	// Make internal node to use with llrb
	ix := makeInternalItem(x)

	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	ms.m.RLock()

	internalFoundInterfaced := tree.Get(ix)
	if internalFoundInterfaced == nil {
		res = nil
	} else {
		// Calculate result with modify
		var itemFoundCopy Item
		var itemFoundCopyInterfaced interface{}
		itemFoundCopy = *(internalFoundInterfaced.(*internalItem).item)
		itemFoundCopyInterfaced = itemFoundCopy
		interfacedResult, modifyResult := modify(itemFoundCopyInterfaced)

		// If update is successful, update internal item
		if modifyResult {
			var itemResult Item = interfacedResult.(Item)
			internalFound := internalFoundInterfaced.(*internalItem)
			*(internalFound.item) = itemResult

			res = itemResult
		} else {
			res = nil
		}
	}

	ms.m.RUnlock()

	return res
}

func (ms *Memstore) UpdateWithIndexes(x Item, index string, modify (func(Item) (Item, bool)) ) (res interface{}) {
	// Make internal node to use with llrb
	ix := makeInternalItem(x)

	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	var ok bool
	var itemCopy, itemResult Item

	ms.m.Lock()

	internalFoundInterfaced := tree.Get(ix)
	if internalFoundInterfaced == nil {
		ok = false
	} else {
		// Modify copy using user-provided function
		var internalFound *internalItem = internalFoundInterfaced.(*internalItem)
		itemCopy = *(internalFound.item)
		itemResult, ok = modify(itemCopy)

		// If found and update would be successful, delete using copy item then add modified one to all tables
		if ok {
			// Delete from all trees
			for _, tree := range ms.indexTree {
				tree.Delete(internalFound)
			}

			// Add to every internal tree
			ix := makeInternalItem(itemResult)
			for _, tree := range ms.indexTree {
				tree.ReplaceOrInsert(ix)
			}
		}
	}

	ms.m.Unlock()

	if ok {
		return itemResult
	} else {
		return nil
	}
}
