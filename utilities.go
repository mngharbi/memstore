package memstore

import (
	"github.com/mngharbi/GoLLRB/llrb"
)

// Make internal item (to work with llrb) from external item
func makeInternalItem(item Item) llrb.Item {
	itemCopy := item
	return &internalItem{
		item: &itemCopy,
	}
}

// Delete item from a certain tree
func (ms *Memstore) delete(x *internalItem, tree *llrb.LLRB) *internalItem {
	deleted := tree.Delete(x)
	if deleted == nil {
		return nil
	}
	return deleted.(*internalItem)
}