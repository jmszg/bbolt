package bbolt

import (
	"bytes"
	"fmt"
	"sort"
)

// Cursor represents an iterator that can traverse over all key/value pairs in a bucket
// in lexicographical order.
// Cursors see nested buckets with value == nil.
// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
//
// Keys and values returned from the cursor are only valid for the life of the transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.

// Cursor 表示一个迭代器，它可以按照字典顺序遍历桶中的所有键值对。
// cursor 能访问value=nil的嵌套bucket
// curorss 可以从事务中获取，只要事务是打开的就有效
// 从游标返回的键和值仅在事务的生命周期内有效。
// 在使用游标遍历时更改数据可能会导致其无效并返回意外的键和/或值。 您必须在更改数据后重新定位游标。
type Cursor struct {
	// 保存需要遍历的bucket的指针
	bucket *Bucket
	// 保存遍历搜索的路径
	stack []elemRef
}

// Bucket returns the bucket that this cursor was created from.
// 返回创建此游标的存储桶。
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
// 移动游标到bucket的第一项，并返回其key和value。
// 如果bucket是空的，将返回一个nil的key和value
// 返回的key和value仅在事务生命周期内有效
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.first()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) first() (key []byte, value []byte, flags uint32) {
	// 重置stack
	c.stack = c.stack[:0]
	// 加载根节点
	p, n := c.bucket.pageNode(c.bucket.root)
	// 根节点信息加入到stack中
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	// 一路向下走到第一个叶子节点，内部通过for循环走到最左侧的叶子节点
	c.goToFirstElementOnTheStack()

	// If we land on an empty page then move to the next value.
	// https://github.com/boltdb/bolt/issues/450
	// 叶子节点中没有元素，移动到下一个叶子节点
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	// 获取最左侧叶子节点中的第一个key-value
	k, v, flags := c.keyValue()
	// 如果是一个桶节点，value为空
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil, flags
	}
	// 存储的是数据，返回key-value，类型标识
	return k, v, flags
}

// Last moves the cursor to the last item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
// 移动游标到bucket的最后-项，并返回其key和value。
// 如果bucket是空的，将返回一个nil的key和value
// 返回的key和value仅在事务生命周期内有效
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	// 清空stack
	c.stack = c.stack[:0]
	// 加载根节点
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	// 设置index为最后一个元素位置
	ref.index = ref.count() - 1
	// 加入到路径缓存stack中
	c.stack = append(c.stack, ref)
	// 一路向下走到最右侧的节点
	c.last()

	// If this is an empty page (calling Delete may result in empty pages)
	// we call prev to find the last page that is not empty
	// 如果叶子节点中没有元素，则想前移动一个节点
	for len(c.stack) > 0 && c.stack[len(c.stack)-1].count() == 0 {
		c.prev()
	}

	// 如果遍历栈没有元素直接返回
	if len(c.stack) == 0 {
		return nil, nil
	}

	// 获取游标位置的key-value（正常清空就是最右边节点的最后一个）
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	// 返回key，value
	return k, v
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
// 移动游标到下一个节点，并发回key和value
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.prev()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Seek moves the cursor to a given key using a b-tree search and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, a nil key is returned.
// The returned key and value are only valid for the life of the transaction.
// 使用b树搜索，移动游标定位到给定的key值，并返回key和value
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	// 断言数据库是否关闭
	_assert(c.bucket.tx.db != nil, "tx closed")

	k, v, flags := c.seek(seek)

	// If we ended up after the last element of a page then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Delete removes the current key/value under the cursor from the bucket.
// Delete fails if current key/value is a bucket or if the transaction is not writable.
// 从桶里面删除当前游标下的key/value
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}

	// 获取当前游标的key
	key, _, flags := c.keyValue()
	// Return an error if current value is a bucket.
	// 如果当前返回的是一个bucket叶子类型返回错误
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	// 删除key
	c.node().del(key)

	return nil
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
// 移动游标到给定的key并返回
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	// Start from root page/node and traverse to correct page.
	// 清空stack
	c.stack = c.stack[:0]
	// 从根节点开始查找
	c.search(seek, c.bucket.root)

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

// first moves the cursor to the first leaf element under the last page in the stack.
// 移动游标到遍历栈最后一页的第一个叶子页元素
func (c *Cursor) goToFirstElementOnTheStack() {
	for {
		// Exit when we hit a leaf page.
		// 当我们遍历到叶子页时返回
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the first element to the stack.

		// 添加页到遍历栈的第一个元素

		var pgId pgid

		if ref.node != nil {
			pgId = ref.node.inodes[ref.index].pgid
		} else {
			pgId = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgId)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// last moves the cursor to the last leaf element under the last page in the stack.
// 移动游标到遍历栈中最后一页的最后一个叶子元素
func (c *Cursor) last() {
	for {
		// Exit when we hit a leaf page.
		// 当时叶子节点页的时候退出
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the last element in the stack.

		// 根据pgid获取页或者节点

		var pgId pgid

		if ref.node != nil {
			pgId = ref.node.inodes[ref.index].pgid
		} else {
			pgId = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgId)

		// 遍历栈中追加查找到的元素
		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nil.
// 移动到当前元素的下一个叶子元素并且返回key和value，如果游标到了最后一个叶子元素，则返回nil
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		//  切片c.stack中保存的是访问的前缀信息，i从大到小循环，先处理游标最后位置
		// 先在节点内部找到下一个元素，如果节点内部元素已全部访问完，i--走到下一个节点
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			// 移动到节点中的下一个元素
			if elem.index < elem.count()-1 {
				elem.index++
				break
			}
		}

		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		// i为-1说明已经处理完整棵树
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf page.
		// 清理掉已经处理过的节点，保留未访问的节点
		c.stack = c.stack[:i+1]
		// 定位到剩下未处理完成的节点，c.stack[i]即为待处理的节点
		// c.stack[i].index已经定位到节点中位置
		c.goToFirstElementOnTheStack()

		// If this is an empty page then restart and move back up the stack.
		// https://github.com/boltdb/bolt/issues/450
		// 节点中没有数据，继续
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		// 返回key-value
		return c.keyValue()
	}
}

// prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nil key and value are returned.
// 在桶中移动游标到当前元素的前一个元素并返回其key和value
func (c *Cursor) prev() (key []byte, value []byte, flags uint32) {
	// Attempt to move back one element until we're successful.
	// Move up the stack as we hit the beginning of each page in our stack.
	// 现在当前节点内部移动到前一个元素，特殊情况，当已经位于节点的第一个元素位置，这时需要找到当前节点的前驱叶子节点
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		// 调整stack，c.stack[i]为当前活动的节点
		c.stack = c.stack[:i]
	}

	// If we've hit the end then return nil.
	// 已经遍历完整棵树
	if len(c.stack) == 0 {
		return nil, nil, 0
	}

	// Move down the stack to find the last element of the last leaf under this branch.
	// 使用c.last定位到c.stack游标位置节点的最后一个节点的最后一个元素
	c.last()
	return c.keyValue()
}

// search recursively performs a binary search against a given page/node until it finds a given key.

// 对给定的页/节点递归的进行二分查找直到发现给定的key
func (c *Cursor) search(key []byte, pgId pgid) {
	// 根据页id查找页
	p, n := c.bucket.pageNode(pgId)
	// 如果查找到页，校验页类型的合法性
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	// 页和节点的索引记录保存到遍历路径中
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// If we're on a leaf page/node then find the specific node.
	// 如果是在一个叶子页或者叶子节点，则查找这个具体的节点
	if e.isLeaf() {
		// 在叶子节点中查找key
		c.nsearch(key)
		return
	}

	if n != nil {
		// 先从node中查找
		c.searchNode(key, n)
		return
	}
	// 从page中进行查找
	c.searchPage(key, p)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	// 根据key搜索节点
	index := sort.Search(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, n.inodes[index].pgid)
}

func (c *Cursor) searchPage(key []byte, p *page) {
	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, inodes[index].pgid)
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// If we have a node then search its inodes.
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements.
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// keyValue returns the key and value of the current leaf element.
// 返回叶子元素的key和value
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	// 从遍历栈指针
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of page/node then return nil.
	// 如果当前游标指向了页和节点的尾部则返回nil
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// Retrieve value from node.
	// 返回节点的value值
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// Or retrieve value from page.
	// 或者从页当中获取
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

// node returns the node that the cursor is currently positioned on.
// 返回节点当前游标的位置
func (c *Cursor) node() *node {
	// 断言游标的遍历栈长度大于0
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// If the top of the stack is a leaf node then just return it.
	// 如果栈顶元素时叶子节点则直接返回该节点（叶子节点就是最后的存放数据的节点）
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// Start from root and traverse down the hierarchy.
	// 从根开始层次遍历
	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(ref.index)
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef represents a reference to an element on a given page/node.
// 表示页/节点元素的引用
type elemRef struct {
	page  *page // 页指针
	node  *node // node指针
	index int   // 节点索引
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// count returns the number of inodes or page elements.
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
