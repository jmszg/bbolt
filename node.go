package bbolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
// node 表示在内存中反系列化的页
type node struct {
	bucket     *Bucket // 关联一个桶，表示该节点属于哪个桶
	isLeaf     bool    // 是否是叶子节点
	unbalanced bool    // 值为true的话需要考虑在平衡，判断节点中元素是否在一定范围内，否则需要平衡
	spilled    bool    // 值为true的话需要考虑页分裂，节点中的元素太多了，需要进行分裂
	key        []byte  // 对于分支节点，保留的是最小key
	pgid       pgid    // 分支节点关联的页ID
	parent     *node   // 该节点的parent
	children   nodes   // 该节点的孩子节点
	inodes     inodes  // 该节点上保存的数据
}

// root returns the top-level node this node is attached to.
// 返回根节点
func (n *node) root() *node {
	// 如果parent为nil，说明已经为根节点
	if n.parent == nil {
		return n
	}
	// 递归到顶级节点
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
// 返回最小的索引数据节点数
func (n *node) minKeys() int {
	// 如果是叶子节点，最小节点数为1
	if n.isLeaf {
		return 1
	}
	// 否则最小的节点数为2
	return 2
}

// size returns the size of the node after serialization.
// 返回节点系列化后的大小
func (n *node) size() int {
	// sz 页头的大小，slsz 页元素头大小
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		// 页头 + （页元素头 + 索引数据key大小 + 索引数据value大小）* 索引数据个数
		sz += elsz + uintptr(len(item.key)) + uintptr(len(item.value))
	}
	// 返回累加后节点系列化的大小
	return int(sz)
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
// 如果节点的大小比给定的值小返回true，否则返回false
func (n *node) sizeLessThan(v uintptr) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + uintptr(len(item.key)) + uintptr(len(item.value))
		if sz >= v {
			return false
		}
	}
	return true
}

// pageElementSize returns the size of each page element based on the type of node.
// 返回页元素头大小
func (n *node) pageElementSize() uintptr {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// childAt returns the child node at a given index.
// 返回给定索引的子节点
func (n *node) childAt(index int) *node {
	// 叶子节点无子节点
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n)
}

// childIndex returns the index of a given child node.
// 返回孩子节点的索引
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// numChildren returns the number of children.
// 返回孩子个数
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
// 返回相同父节点的下一个节点（下一个兄弟节点）
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
// 返回相同父节点的前一个节点（上一个兄弟节点）
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.

// 插入一个key/value
func (n *node) put(oldKey, newKey, value []byte, pgId pgid, flags uint32) {
	if pgId >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgId (%d) above high water mark (%d)", pgId, n.bucket.tx.meta.pgid))

	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// Find insertion index.
	// 查找插入的索引位置(新的key值大于或者等老的key的索引)
	index := sort.Search(len(n.inodes), func(i int) bool {
		//zgj_test_1 := n.inodes[i].key
		//zgj_test_2 := oldKey
		//zgj_test_3 := bytes.Compare(n.inodes[i].key, oldKey)
		return bytes.Compare(n.inodes[i].key, oldKey) != -1
	})
	// fmt.Println(index)
	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	// 添加容量和移动节点的时候，如果没有精确匹配到老的key，需要新插入。
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		//fmt.Println("append", n.inodes)
		// 将切片的元素整体往后移动一位
		copy(n.inodes[index+1:], n.inodes[index:])
		//fmt.Println("copy", n.inodes)
	}

	// 修改索引节点处的内容
	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value

	inode.pgid = pgId

	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
// 删除key
func (n *node) del(key []byte) {
	// Find index of key.
	// 根据索引二分查找key（节点底层是B+树的节点，按照从小到大的节点）
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// Exit if the key isn't found.
	// 如果没有找到需要删除的key直接返回
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// Delete inode from the node.
	// 删除查找的的节点
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	// 表示node需要重新平衡
	n.unbalanced = true
}

// read initializes the node from a page.
// 根据page初始化node节点
func (n *node) read(p *page) {
	// 赋值pageid
	n.pgid = p.id
	// 是否是叶子节点
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	// 根据节点中元素的个数开辟空间
	n.inodes = make(inodes, int(p.count))

	// 填充节点中的元素数据
	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			// 叶子节点有flag-key-value 的信息
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			// 分支节点只有key-pgid信息，可以把pgid理解为内部节点的value信息，它指向的是该孩子节点的page
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// Save first key so we can find the node in the parent when we spill.
	// 初始化节点n的key值为元素第一key-value中的key，让我们能在spill的时候从父节点找到节点
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// write writes the items onto one or more pages.
// The page should have p.id (might be 0 for meta or bucket-inline page) and p.overflow set
// and the rest should be zeroed.
// 写node到一个或者多个page中，根据节点的内容初始化page，相当于node的序列化
func (n *node) write(p *page) {
	_assert(p.count == 0 && p.flags == 0, "node cannot be written into a not empty page")

	// Initialize page.
	// 初始化节点标识
	if n.isLeaf {
		p.flags = leafPageFlag
	} else {
		p.flags = branchPageFlag
	}

	// 节点中的元素个数超过2^1=65535个，溢出了
	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// Stop here if there are no items to write.
	// 节点中没有元素则直接返回
	if p.count == 0 {
		return
	}

	// Loop over each item and write it to the page.
	// off tracks the offset into p of the start of the next data.
	// off 指向p 开始的像一个数据的指针位置
	// 定位到page中key-value数据要写入的位置，循环写入key-value
	off := unsafe.Sizeof(*p) + n.pageElementSize()*uintptr(len(n.inodes))
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// Create a slice to write into of needed size and advance
		// byte pointer for next iteration.
		// 创建一个切片以写入所需大小并为下一次迭代推进字节指针。
		// 为
		sz := len(item.key) + len(item.value)
		b := unsafeByteSlice(unsafe.Pointer(p), off, 0, sz)
		off += uintptr(sz)

		// Write the page element.
		if n.isLeaf {
			// 如果是叶子节点

			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// Write data for the element to the end of the page.
		// 在页尾写入元素
		l := copy(b, item.key)
		copy(b[l:], item.value)
	}

	// DEBUG ONLY: n.dump()
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
// 如果合适建一个节点拆分成多个较小的节点，应该只从spill（）函数中调用
func (n *node) split(pageSize uintptr) []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		// 把node拆分成两个页
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		// 如果无法拆分，则退出循环
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		// 再次迭代拆分b
		node = b
	}

	return nodes
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
// 如果合适，将node拆分成两个较小的节点，应该只在split（）函数中被调用
func (n *node) splitTwo(pageSize uintptr) (*node, *node) {
	// Ignore the split if the page doesn't have at least enough nodes for
	// two pages or if the nodes can fit in a single page.
	// 忽略拆分，如果页没有足够的节点拆分成两个页，或者节点能在单独的一个页
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// Determine the threshold before starting a new node.
	// 开始创建一个新节点之前确定填充率阀值
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// Determine split position and sizes of the two pages.
	// 确定拆分位置和两个页的大小
	splitIndex, _ := n.splitIndex(threshold)

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	// 将节点拆分成个独立的节点，如果他们没有父节点，我们需要创建一个。
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// Create a new node and add it to the parent.
	// 创建一个节点，并添加到其父节点下面
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	// 拆分inodes
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	// 更新统计信息
	n.bucket.tx.stats.IncSplit(1)

	return n, next
}

// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
// 查找页根据给定阀值需要拆分的位置，返回索引位置，和第一个页的大小
func (n *node) splitIndex(threshold int) (index, sz uintptr) {
	// 页头的大小
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	// 一个页至少有两个关键字页
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = uintptr(i)
		inode := n.inodes[i]
		elsize := n.pageElementSize() + uintptr(len(inode.key)) + uintptr(len(inode.value))

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if index >= minKeysPerPage && sz+elsize > uintptr(threshold) {
			break
		}

		// Add the element size to the total size.
		// 累加元素大小到总大小里面
		sz += elsize
	}

	return
}

// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
// 写脏页、拆分节点，如果无法分配脏页则发回错误
func (n *node) spill() error {
	var tx = n.bucket.tx
	// 如果节点已经处理过了直接返回
	if n.spilled {
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	// 先对孩子节点进行排序
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		// 先处理当前节点的孩子节点，处理过程是一个自顶向上的过程
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	// 处理到这里的时候，节点n的孩子节点已经全部处理完并保存到分配的脏页page中，脏页page已经被保存到
	// tx.pages中了，所以可以将n.children设置为nil,GC可以回收孩子节点node了
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	// 按给定的pageSize对节点n进行切分
	var nodes = n.split(uintptr(tx.db.pageSize))
	for _, node := range nodes {
		// Add node's page to the freelist if it's not new.
		// 不是新分配的node,也就是该node在db中有关联的page,需要将node对应的page加入到freelist中
		// 在下一次开启读写事务时，此page可以被重新复用
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// Allocate contiguous space for the node.
		// 为节点n分配page,注意分配的page页此时还是位于内存中，尚未刷新到磁盘，
		p, err := tx.allocate((node.size() + tx.db.pageSize - 1) / tx.db.pageSize)
		if err != nil {
			return err
		}

		// Write the node.
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		// 分配的page id值赋值给node
		node.pgid = p.id
		// 将node中的元素内容序列化化到page p中
		node.write(p)
		// 标记节点node已经spill处理过了
		node.spilled = true

		// Insert into parent inodes.
		// 将节点node添加到它的父节点中
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// Update the statistics.
		tx.stats.IncSpill(1)
	}

	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	// 对根节点进行spill
	if n.parent != nil && n.parent.pgid == 0 {
		// 先将children设置nil, 防止又递归spill
		n.children = nil
		// 对根节点进行spill
		return n.parent.spill()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	// 如果节点平衡则返回
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Update statistics.
	// 统计平衡次数
	n.bucket.tx.stats.IncRebalance(1)

	// Ignore if node is above threshold (25%) and has enough keys.
	// 阀值threshold大小为数据库页大小的1/4,page大小为4KB，即threshold为1KB
	var threshold = n.bucket.tx.db.pageSize / 4
	// 当n节点所占用空间的大小大于1KB 并且节点中的元素个数超过最小值，对于页节点是1个，
	// 对于分支节点是2个。即叶子节点元素最少为2个，分支节点元素至少为3个，不用进行合并平衡处理
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// Root node has special handling.
	// n 是根节点的特殊情况处理
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		// 如果改根节点是页分支节点，并且只有一个子节点，则合并其唯一的子节点。
		if !n.isLeaf && len(n.inodes) == 1 {
			// Move root's child up.
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Reparent all child nodes being moved.
			// 修改inodes的父节点，从原来的child改为现在的n
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			// 解除child关联的父节点
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			// 释放child节点，加入freelist
			child.free()
		}

		return
	}

	// If node has no keys then just remove it.
	// 如果节点n中没有任何元素，则移除它
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		// 重新平衡父节点
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	// 下面将n节点和它的兄弟节点进行合并，默认是和左边的兄弟进行合并，但如果n节点本来就是左边的第一个节点
	// 也就是他没有左兄弟节点，这种情况将n和它的右兄弟进行合并
	var target *node
	// 判断n是不是第一个节点
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		// 选择右边的兄弟进行合并
		target = n.nextSibling()
	} else {
		// 选择左边的兄弟进行合并
		target = n.prevSibling()
	}

	// If both this node and the target node are too small then merge them.
	// 与右兄弟进行合并
	if useNextSibling {
		// Reparent all child nodes being moved.
		// 调整target中所有孩子的父节点，从target中调整为n,因为target和n合并
		// 合并之后节点为n
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				// 从旧的父节点中移除child
				child.parent.removeChild(child)
				// 调整child的新父节点为当前节点n
				child.parent = n
				// 将child加入到节点n的孩子集合中
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes from target and remove target.
		// 将target中的数据合并到节点n中
		n.inodes = append(n.inodes, target.inodes...)
		// 从当前节点n的父节点中删除target key
		n.parent.del(target.key)
		// 将target中B+tree中移除
		n.parent.removeChild(target)
		// 缓存的node集合中删除target
		delete(n.bucket.nodes, target.pgid)
		// 释放target占用的空间
		target.free()
	} else {
		// Reparent all child nodes being moved.
		// 与左兄弟节点合并，将当前节点n合并到target节点
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				// 从旧的父节点中移除child
				child.parent.removeChild(child)
				// 调整child的新的父节点为target
				child.parent = target
				// 将child加入到target节点的孩子集群中
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes to target and remove node.
		// 当前节点的数据合并到target节点中
		target.inodes = append(target.inodes, n.inodes...)
		// 从当前n节点的父节点中删除n的 key
		n.parent.del(n.key)
		// 将n从B+树中移除
		n.parent.removeChild(n)
		// 缓存的node集合中删除n
		delete(n.bucket.nodes, n.pgid)
		// 释放n占用的空间
		n.free()
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	// 合并操作会导致节点的删除，因此向上递归看是否需要进一步平衡调节
	n.parent.rebalance()
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
// 使节点指向空的堆内存空间
func (n *node) dereference() {
	// 如果key不为空，清空key的值
	if n.key != nil {
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	// 清空数据节点的key和value
	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	// 递归清空清空key和value
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	// 更新统计信息
	n.bucket.tx.stats.IncNodeDeref(1)
}

// free adds the node's underlying page to the freelist.
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}
*/

func compareKeys(left, right []byte) int {
	return bytes.Compare(left, right)
}

type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1
}

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
// inode 表示一个内部节点的节点，它可以指向页内的元素或者指向一个还没有被添加到页的元素
type inode struct {
	// 表示是否是子桶叶子节点还是普通叶子节点
	flags uint32
	// 当inode为分支元素时才有值
	pgid pgid
	key  []byte
	// 当inode为叶子节点时才有值
	value []byte
}

type inodes []inode
