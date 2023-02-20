package bbolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// txPending holds a list of pgids and corresponding allocation txns
// that are pending to be freed.
// txPending持有一个页列表和相应分配的txns等待释放
type txPending struct {
	// 页id
	ids []pgid
	// 分配的事务id
	alloctx []txid // txids allocating the ids
	// 开始事务最后匹配的页
	lastReleaseBegin txid // beginning txid of last matching releaseRange
}

// pidSet holds the set of starting pgids which have the same span size
type pidSet map[pgid]struct{}

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
// freelist 表示可用于分配的所有页面的列表。
// 它还跟踪已被释放但仍被开启的事物使用。
type freelist struct {
	// 空闲列表的类型
	freelistType FreelistType // freelist type
	// 所有可用的空闲页
	ids []pgid // all free and available free page ids.
	// 申请的页
	allocs map[pgid]txid // mapping of txid that allocated a pgid.
	// 即将清空的页
	pending map[txid]*txPending // mapping of soon-to-be free page ids by tx.
	// 所有空闲和等待释放的页id
	cache       map[pgid]struct{} // fast lookup of all free and pending page ids.
	freemaps    map[uint64]pidSet // key is the size of continuous pages(span), value is a set which contains the starting pgids of same size
	forwardMap  map[pgid]uint64   // key is start pgid, value is its span size
	backwardMap map[pgid]uint64   // key is end pgid, value is its span size
	// 空闲列表分配函数
	allocate       func(txid txid, n int) pgid // the freelist allocate func
	free_count     func() int                  // the function which gives you free page number
	mergeSpans     func(ids pgids)             // the mergeSpan func
	getFreePageIDs func() []pgid               // get free pgids func
	readIDs        func(pgids []pgid)          // readIDs func reads list of pages and init the freelist
}

// newFreelist returns an empty, initialized freelist.
// 初始化一个空的空闲列表
func newFreelist(freelistType FreelistType) *freelist {
	f := &freelist{
		freelistType: freelistType,
		allocs:       make(map[pgid]txid),
		pending:      make(map[txid]*txPending),
		cache:        make(map[pgid]struct{}),
		freemaps:     make(map[uint64]pidSet),
		forwardMap:   make(map[pgid]uint64),
		backwardMap:  make(map[pgid]uint64),
	}

	if freelistType == FreelistMapType {
		f.allocate = f.hashmapAllocate
		f.free_count = f.hashmapFreeCount
		f.mergeSpans = f.hashmapMergeSpans
		f.getFreePageIDs = f.hashmapGetFreePageIDs
		f.readIDs = f.hashmapReadIDs
	} else {
		f.allocate = f.arrayAllocate
		f.free_count = f.arrayFreeCount
		f.mergeSpans = f.arrayMergeSpans
		f.getFreePageIDs = f.arrayGetFreePageIDs
		f.readIDs = f.arrayReadIDs
	}

	return f
}

// size returns the size of the page after serialization.
// 返回初始化后的空闲列表页的大小
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		// 如何大小大于2个字节，第一个元素用于存放数量
		n++
	}
	return int(pageHeaderSize) + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist
// 返回空闲列表中的页数
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// arrayFreeCount returns count of free pages(array version)
// 返回空闲页页数
func (f *freelist) arrayFreeCount() int {
	return len(f.ids)
}

// pending_count returns count of pending pages
// 返回待处理页的个数
func (f *freelist) pending_count() int {
	var count int
	for _, txp := range f.pending {
		count += len(txp.ids)
	}
	return count
}

// copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
// 复制所有的空闲页和待处理页到一个列表中
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, txp := range f.pending {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	mergepgids(dst, f.getFreePageIDs(), m)
}

// arrayAllocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
// 返回给定大小的连续页面列表的起始页id，如果没有连续块则返回0
func (f *freelist) arrayAllocate(txid txid, n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		// 校验id的合法性
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		// 如果不是连续的重置初始页
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		// 查找连续的块，删除并返回它
		if (id-initial)+1 == pgid(n) {
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// Remove from the free cache.
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}
			f.allocs[initial] = txid
			return initial
		}

		previd = id
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
// 释放给定事务id的页面及其溢出
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// Free page and all its overflow pages.
	// 释放事务相关的所有页以及溢出的页
	// 获取即将清除的也缓存，没有则
	txp := f.pending[txid]
	if txp == nil {
		txp = &txPending{}
		f.pending[txid] = txp
	}

	// 从申请页里面去查找，如果找到则释放
	allocTxid, ok := f.allocs[p.id]
	if ok {
		delete(f.allocs, p.id)
	} else if (p.flags & freelistPageFlag) != 0 {
		// Freelist is always allocated by prior tx.
		// 空闲列表总是由前一个tx申请分配
		allocTxid = txid - 1
	}

	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// Verify that page is not already free.
		// 校验页是否已经释放
		if _, ok := f.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		// 添加空闲列表并且缓存
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		f.cache[id] = struct{}{}
	}
}

// release moves all page ids for a transaction id (or older) to the freelist.
// 将事务id的所有页移动到空闲列表中
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, txp := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
// 将范围 [begin,end] 内分配的待处理页面移动到空闲列表。
func (f *freelist) releaseRange(begin, end txid) {
	if begin > end {
		return
	}
	var m pgids
	for tid, txp := range f.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// rollback removes the pages from a given pending tx.
// 根据挂起的事务id删除页
func (f *freelist) rollback(txid txid) {
	// Remove page ids from cache.
	// 索引出挂起页
	txp := f.pending[txid]
	if txp == nil {
		return
	}

	// 删除挂起页
	var m pgids
	for i, pgid := range txp.ids {
		// 删除需要释放的页
		delete(f.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			// 不需要挂起的页，重存分配列表
			f.allocs[pgid] = tx
		} else {
			// Freed page was allocated by this txn; OK to throw away.
			// 要释放的页由此txn分配则丢弃
			m = append(m, pgid)
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	// 删除由此事务分配的挂起空闲页，标记为释放
	delete(f.pending, txid)
	// 执行回调函数
	f.mergeSpans(m)
}

// freed returns whether a given page is in the free list.
// 返回一个页是否在空闲列表中
func (f *freelist) freed(pgid pgid) bool {
	_, ok := f.cache[pgid]
	return ok
}

// read initializes the freelist from a freelist page.
// 从空闲页初始化空闲列表
func (f *freelist) read(p *page) {
	if (p.flags & freelistPageFlag) == 0 {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.id, p.typ()))
	}
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	var idx, count = 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		c := *(*pgid)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
		count = int(c)
		if count < 0 {
			panic(fmt.Sprintf("leading element count %d overflows int", c))
		}
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil
	} else {
		var ids []pgid
		data := unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
		unsafeSlice(unsafe.Pointer(&ids), data, count)

		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]pgid, count)
		copy(idsCopy, ids)
		// Make sure they're sorted.
		sort.Sort(pgids(idsCopy))

		f.readIDs(idsCopy)
	}
}

// arrayReadIDs initializes the freelist from a given list of ids.
// 根据页号集合初始化空闲列表
func (f *freelist) arrayReadIDs(ids []pgid) {
	f.ids = ids
	f.reindex()
}

func (f *freelist) arrayGetFreePageIDs() []pgid {
	return f.ids
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
// 页ids写入空闲页，所有的空闲和待处理ids都保存到磁盘，因为如果程序奔溃，所有的待处理ids都将释放。
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	// 更新页头标识
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := f.count()
	if l == 0 {
		p.count = uint16(l)
	} else if l < 0xFFFF {
		p.count = uint16(l)
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l)
		f.copyall(ids)
	} else {
		p.count = 0xFFFF
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l+1)
		ids[0] = pgid(l)
		f.copyall(ids[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
// 从页面中获取空闲列表，并过滤掉待处理的
func (f *freelist) reload(p *page) {
	f.read(p)

	// Build a cache of only pending pages.
	// 构建一个待处理页的缓存
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	// 检查在空闲列表中的页，并构建一个可用的空闲列表包含任何不在待处理列表中的页
	var a []pgid
	for _, id := range f.getFreePageIDs() {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// noSyncReload reads the freelist from pgids and filters out pending items.
// 读取空闲列表中的pgids并且过滤掉待处理的
func (f *freelist) noSyncReload(pgids []pgid) {
	// Build a cache of only pending pages.
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range pgids {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
// 根据可用和待处理的空闲列表构建空闲缓存
func (f *freelist) reindex() {
	ids := f.getFreePageIDs()
	f.cache = make(map[pgid]struct{}, len(ids))
	for _, id := range ids {
		f.cache[id] = struct{}{}
	}
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			f.cache[pendingID] = struct{}{}
		}
	}
}

// arrayMergeSpans try to merge list of pages(represented by pgids) with existing spans but using array
func (f *freelist) arrayMergeSpans(ids pgids) {
	sort.Sort(ids)
	f.ids = pgids(f.ids).merge(ids)
}
