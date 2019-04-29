/*
	All exported functions
*/

package memstore

import (
	"github.com/mngharbi/GoLLRB/llrb"
)

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

func (ms *Memstore) Add(x Item) {
	// Make internal node to use in llrb
	ix := makeInternalItem(x)

	ms.m.Lock()

	// Add to every internal tree
	for _, tree := range ms.indexTree {
		tree.ReplaceOrInsert(ix)
	}

	ms.m.Unlock()
}

func getFromTree(ix llrb.Item, tree *llrb.LLRB) *Item {
	ifound := tree.Get(ix)
	if ifound == nil {
		return nil
	}
	return ifound.(*internalItem).item
}

func (ms *Memstore) AddOrGet(x Item) Item {
	// Make internal node to use in llrb
	ix := makeInternalItem(x)

	var res *Item

	ms.m.Lock()

	// Search for item in all trees
	for _, tree := range ms.indexTree {
		res = getFromTree(ix, tree)
		if res != nil {
			break
		}
	}

	// Add to internal trees only if not found
	if res == nil {
		res = ix.(*internalItem).item
		for _, tree := range ms.indexTree {
			tree.ReplaceOrInsert(ix)
		}
	}

	ms.m.Unlock()

	if res == nil {
		return nil
	} else {
		return *res
	}
}

func (ms *Memstore) Delete(x Item, index string) Item {
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

func (ms *Memstore) GetRange(from, to Item, index string, test func(Item) bool) {
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

func (ms *Memstore) UpdateData(x Item, index string, modify func(Item) (Item, bool)) (res Item) {
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
		internalFound := internalFoundInterfaced.(*internalItem)
		itemFoundCopy = *(internalFound.item)
		itemResult, modifyResult := modify(itemFoundCopy)

		// If update is successful, update internal item
		if modifyResult {
			*(internalFound.item) = itemResult
			res = itemResult
		} else {
			res = nil
		}
	}

	ms.m.RUnlock()

	return res
}

func (ms *Memstore) ApplyData(x Item, index string, run func(Item) bool) (res Item) {
	// Make internal node to use with llrb
	ix := makeInternalItem(x)

	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	ms.m.RLock()
	defer func() { ms.m.RUnlock() }()

	internalFoundInterfaced := tree.Get(ix)
	if internalFoundInterfaced == nil {
		res = nil
	} else {
		// Calculate result with modify
		var itemFoundCopy Item
		internalFound := internalFoundInterfaced.(*internalItem)
		itemFoundCopy = *(internalFound.item)
		runResult := run(itemFoundCopy)

		// If result is successful, return item
		if runResult {
			res = itemFoundCopy
		} else {
			res = nil
		}
	}

	return res
}

func (ms *Memstore) UpdateWithIndexes(x Item, index string, modify func(Item) (Item, bool)) (res Item) {
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

func (ms *Memstore) UpdateDataSubset(items []Item, index string, modify func(Item) (Item, bool)) (res []Item) {
	// Make internal nodes to use with llrb
	internalItems := []llrb.Item{}
	for _, it := range items {
		internalItems = append(internalItems, makeInternalItem(it))
	}

	// Get corresponding tree
	tree := ms.indexTree[index]
	if tree == nil {
		return nil
	}

	ms.m.RLock()

	for _, iitem := range internalItems {
		internalFoundInterfaced := tree.Get(iitem)
		if internalFoundInterfaced == nil {
			res = append(res, nil)
		} else {
			// Calculate result with modify
			var itemFoundCopy Item
			internalFound := internalFoundInterfaced.(*internalItem)
			itemFoundCopy = *(internalFound.item)
			itemResult, modifyResult := modify(itemFoundCopy)

			// If update is successful, update internal item
			if modifyResult {
				*(internalFound.item) = itemResult
				res = append(res, itemResult)
			} else {
				res = append(res, nil)
			}
		}
	}

	ms.m.RUnlock()

	return res
}
