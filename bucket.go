package bbolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	// key的最大长度，以字节为单位
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	// value的最大长度，以字节为单位
	MaxValueSize = (1 << 31) - 2
)

// bucket头的大小
const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	// 最小填充率
	minFillPercent = 0.1
	// 最大填充率
	maxFillPercent = 1.0
)

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
// 默认填充率
const DefaultFillPercent = 0.5

// Bucket represents a collection of key/value pairs inside the database.
// Bucket 表示数据库中键/值对的集合,也就是B+树
type Bucket struct {
	// b+ 树根节点的信息，主要是根节点的pageid
	*bucket
	// 操作bucket的事务句柄
	tx *Tx // the associated transaction
	// 子bucket
	buckets map[string]*Bucket // subbucket cache
	// 内敛page
	page *page // inline page reference
	// Bucket管理的树的根节点
	rootNode *node // materialized node for the root page.
	// node节点缓存
	nodes map[pgid]*node // node cache

	// Sets the threshold for filling nodes when they split. By default,
	// the bucket will fill to 50% but it can be useful to increase this
	// amount if you know that your write workloads are mostly append-only.
	//
	// This is non-persisted across transactions so it must be set in every Tx.
	// 填充率
	FillPercent float64
}

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
// bucket 表示存储桶的文件表示。
type bucket struct {
	// 桶根基本的页id
	root pgid // page id of the bucket's root-level page
	// 桶序列号，单调递增
	sequence uint64 // monotonically incrementing, used by NextSequence()
}

// newBucket returns a new bucket associated with a transaction.
// 返回一个与事务关联的新桶
// bucket构造函数
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	// 如果是写事务则创建子桶和节点缓存
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// Tx returns the tx of the bucket.
// 返回事务的桶
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root returns the root of the bucket.
// 返回bucket的根
func (b *Bucket) Root() pgid {
	return b.root
}

// Writable returns whether the bucket is writable.
// 返回桶是否可写
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
// 创建和桶相关的游标，只有当事务打开的时候游标才有效，事务关闭后不使用游标
func (b *Bucket) Cursor() *Cursor {
	// Update transaction statistics.
	// 更新事务游标统计
	b.tx.stats.IncCursorCount(1)

	// Allocate and return a cursor.
	// 创建并返回一个游标
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
// 按照名称来检索bucket，如果返回nil表示不存在。bucket仅在事务的生命周期内有效
// 在bucket中查找给定名称的bucket
func (b *Bucket) Bucket(name []byte) *Bucket {
	// 先从缓存中查找，如果查找到则直接返回
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// Move cursor to key.
	// 创建一个游标，使用游标进行遍历查找
	c := b.Cursor()
	// 在B+树中查找key为name的节点
	k, v, flags := c.seek(name)

	// Return nil if the key doesn't exist or it is not a bucket.
	// 如果不存在或者不是一个bucket节点
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// Otherwise create a bucket and cache it.
	// 找到了给定名称的桶，创建一个Bucket
	var child = b.openBucket(v)
	if b.buckets != nil {
		// 缓存起来
		b.buckets[string(name)] = child
	}

	return child
}

// Helper method that re-interprets a sub-bucket value
// from a parent into a Bucket
func (b *Bucket) openBucket(value []byte) *Bucket {
	// 创建一个事务关联的桶
	var child = newBucket(b.tx)

	// Unaligned access requires a copy to be made.
	// 内存对齐的掩码，如果内存地址是对齐的，则需要是掩码的倍数。再二进制中也就是掩码位置后面都是0
	// 比如4字节对齐，说明二进制以00结尾。
	const unalignedMask = unsafe.Alignof(struct {
		bucket
		page
	}{}) - 1
	//zgj_test_1 := unalignedMask
	//zgj_test_2 := uintptr(unsafe.Pointer(&value[0]))
	//zgj_test_3 := uintptr(unsafe.Pointer(&value[0])) & unalignedMask
	//fmt.Println(zgj_test_1, zgj_test_2, zgj_test_3)
	// 这个地方检测内存地址是否对齐，采用与的方式，来判断掩码位是否都为0，如果都为0，说明是内存按照规定的大小对齐的，
	// 否则不是内存对齐的。
	unaligned := uintptr(unsafe.Pointer(&value[0]))&unalignedMask != 0
	if unaligned {
		// 如果不是内存对齐的，使用切片拷贝到连续的对齐的内存空间中
		value = cloneBytes(value)
	}

	// If this is a writable transaction then we need to copy the bucket entry.
	// Read-only transactions can point directly at the mmap entry.
	if b.tx.writable && !unaligned {
		// 如果是写事务，需要拷贝bucket到一个实体
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		// 否则，直接引用其地址
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// Save a reference to the inline page if the bucket is inline.
	// 如果bucket是内敛，指向一个内敛页
	if child.root == 0 {
		// 这个地方保存的是引用bucket头信息之后的内容
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
// 创建一个桶，如果名称已存在，则返回错误
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	// 异常情况处理：1 db已关闭   2 非读写事务，不支持创建桶  3 桶的名称为空
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// Move cursor to correct position.
	// 创建一个游标对象
	c := b.Cursor()
	// 在B+树中查找key的节点
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key.
	// 如果已存在并且是个桶
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		// 桶的名称不能和数据中的key重名
		return nil, ErrIncompatibleValue
	}

	// Create empty, inline bucket.
	// 创建一个新的Bucket
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	// Insert into node.
	// 对传入的key进行深度拷贝，传入的key是用户空间的，防止用户后续修改key
	key = cloneBytes(key)
	// 将bucket加入到节点c.node()中
	c.node().put(key, key, value, 0, bucketLeafFlag)

	// Since subbuckets are not allowed on inline buckets, we need to
	// dereference the inline page, if it exists. This will cause the bucket
	// to be treated as a regular, non-inline bucket for the rest of the tx.
	//  当前桶已经有子桶，不能被内敛
	b.page = nil

	// 不直接返回bucket，主要是要设置bucket.root的值
	return b.Bucket(key), nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exist, or if the key represents a non-bucket value.
// 根据key删除桶，如果不存在或不是一个桶值返回错误
func (b *Bucket) DeleteBucket(key []byte) error {
	// 异常情况检查
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	// 创建一个游标对桶进行遍历查找给定key的节点
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if bucket doesn't exist or is not a bucket.
	// 如果桶不存在
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		// 存在给定名称的key，但它不是桶的名称
		return ErrIncompatibleValue
	}

	// Recursively delete all child buckets.
	// 递归遍历删除即将要删除的桶的所有的子bucket
	child := b.Bucket(key)
	err := child.ForEachBucket(func(k []byte) error {
		if err := child.DeleteBucket(k); err != nil {
			return fmt.Errorf("delete bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove cached copy.
	// 删除缓存map中的桶
	delete(b.buckets, string(key))

	// Release all bucket pages to freelist.
	child.nodes = nil
	child.rootNode = nil
	// 释放关联的page
	child.free()

	// Delete the node if we have a matching key.
	// 从叶子节点中删除该桶key
	c.node().del(key)

	return nil
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
// 根据key返回桶中的value值
func (b *Bucket) Get(key []byte) []byte {
	// 使用桶中的游标定位当前key
	k, v, flags := b.Cursor().seek(key)

	// Return nil if this is a bucket.
	// 如果定位到一个桶则返回nil
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	// 如果定位不匹配，返回nil
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too large, or if the value is too large.
// 往桶里面添加值，如果之前存在，则先前的值将被覆盖，提供的值必须在事务周期内有效
func (b *Bucket) Put(key []byte, value []byte) error {
	// 校验参数和写入数据前的前置条件
	if b.tx.db == nil {
		// 如果事务关闭了返回ErrTxClosed
		return ErrTxClosed
	} else if !b.Writable() {
		// 如果不可读写，返回ErrTxNotWritable
		return ErrTxNotWritable
	} else if len(key) == 0 {
		// 如果key的长度为0返回ErrKeyRequired
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		// 如果key超长了，则返回ErrKeyTooLarge
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		// 如果值大小超长了则返回ErrValueTooLarge
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	// 通过游标定位当前key的位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key with a bucket value.
	// 如果存在相同key的bucket且节点类型为bucket则返回ErrIncompatibleValue
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Insert into node.
	// 插入一个节点
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0)

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
// 从桶里面删除一个key/value键值对
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		// 如果数据库已关闭返回ErrTxClosed
		return ErrTxClosed
	} else if !b.Writable() {
		// 如果不可读写返回ErrTxNotWritable
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	// 移动游标到当前位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return nil if the key doesn't exist.
	// 如果不存在返回nil
	if !bytes.Equal(key, k) {
		return nil
	}

	// Return an error if there is already existing bucket value.
	// 如果是一个已存在的bucket值返回错误
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Delete the node if we have a matching key.
	// 删除匹配的key
	c.node().del(key)

	return nil
}

// Sequence returns the current integer for the bucket without incrementing it.
func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

// SetSequence updates the sequence number for the bucket.
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Set the sequence.
	b.bucket.sequence = v
	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// ForEach executes a function for each key/value pair in a bucket.
// Because ForEach uses a Cursor, the iteration over keys is in lexicographical order.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// 遍历桶执行回调函数
func (b *Bucket) ForEachBucket(fn func(k []byte) error) error {
	// 如果数据库已关闭则返回ErrTxClosed
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, _, flags := c.first(); k != nil; k, _, flags = c.next() {
		if flags&bucketLeafFlag != 0 {
			if err := fn(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// Stats returns stats on a bucket.
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int, pgstack []pgid) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			// used totals the used bytes for the page
			used := pageHeaderSize

			if p.count != 0 {
				// If page has any elements, add all element headers.
				used += leafPageElementSize * uintptr(p.count-1)

				// Add all element key, value sizes.
				// The computation takes advantage of the fact that the position
				// of the last element's key/value equals to the total of the sizes
				// of all previous elements' keys and values.
				// It also includes the last element's header.
				lastElement := p.leafPageElement(p.count - 1)
				used += uintptr(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}

			if b.root == 0 {
				// For inlined bucket just update the inline stats
				s.InlineBucketInuse += int(used)
			} else {
				// For non-inlined bucket update all the leaf stats
				s.LeafPageN++
				s.LeafInuse += int(used)
				s.LeafOverflowN += int(p.overflow)

				// Collect stats from sub-buckets.
				// Do that by iterating over all element headers
				// looking for the ones with the bucketLeafFlag.
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						// For any bucket element, open the element value
						// and recursively call Stats on the contained bucket.
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used totals the used bytes for the page
			// Add header and all element headers.
			used := pageHeaderSize + (branchPageElementSize * uintptr(p.count-1))

			// Add size of all keys and values.
			// Again, use the fact that last element's position equals to
			// the total of key, value sizes of all previous elements.
			used += uintptr(lastElement.pos + lastElement.ksize)
			s.BranchInuse += int(used)
			s.BranchOverflowN += int(p.overflow)
		}

		// Keep track of maximum page depth.
		if depth+1 > s.Depth {
			s.Depth = depth + 1
		}
	})

	// Alloc stats can be computed from page counts and pageSize.
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// Add the max depth of sub-buckets to get total nested depth.
	s.Depth += subStats.Depth
	// Add the stats for all sub-buckets
	s.Add(subStats)
	return s
}

// forEachPage iterates over every page in a bucket, including inline pages.
func (b *Bucket) forEachPage(fn func(*page, int, []pgid)) {
	// If we have an inline page then just use that.
	if b.page != nil {
		fn(b.page, 0, []pgid{b.root})
		return
	}

	// Otherwise traverse the page hierarchy.
	b.tx.forEachPage(b.root, fn)
}

// forEachPageNode iterates over every page (or node) in a bucket.
// This also includes inline pages.
// 迭代bucket中的每一页，包括内敛页
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// If we have an inline page or root node then just use that.
	// 如果有内敛页或者根节点则使用它
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// Execute function.
	fn(p, n, depth)

	// Recursively loop over children.
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// spill writes all the nodes for this bucket to dirty pages.
// 将此桶的所有节点写入脏页
func (b *Bucket) spill() error {
	// Spill all child buckets first.
	// 对所有的子bucket进行spill处理
	for name, child := range b.buckets {
		// If the child bucket is small enough and it has no child buckets then
		// write it inline into the parent bucket's page. Otherwise spill it
		// like a normal bucket and make the parent value a pointer to the page.
		var value []byte
		// 判断桶是否是内敛的
		if child.inlineable() {
			// 可以被内敛则释放掉child
			child.free()
			// 将child序列化内容放入到它父节点的page中
			value = child.write()
		} else {
			// 递归对child的子bucket进行spill处理
			if err := child.spill(); err != nil {
				return err
			}

			// Update the child bucket header in this bucket.
			// 重新获取child的bucket信息，因为对child进行spill处理后
			// 因为有分裂操作，可能会导致child.bucket发生变化
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// Skip writing the bucket if there are no materialized nodes.
		// 跳过没有物化的节点
		if child.rootNode == nil {
			continue
		}

		// Update parent node.
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		// 更新name的value
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there's not a materialized root node.
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	// 从根节点进行溢出处理
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// Update the root node for this bucket.
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	// 更新b的pgid
	b.root = b.rootNode.pgid

	return nil
}

// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
// 如果bucket足够小并且不包含子桶，则可以内敛写入
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// Bucket must only contain a single leaf node.
	// bucket只能包含一个叶子节点
	if n == nil || !n.isLeaf {
		return false
	}

	// Bucket is not inlineable if it contains subbuckets or if it goes beyond
	// our threshold for inline bucket size.
	// 如果桶包含子桶或者超过了内敛桶大小的阀值则不是内敛桶
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + uintptr(len(inode.key)) + uintptr(len(inode.value))

		if inode.flags&bucketLeafFlag != 0 {
			return false
		} else if size > b.maxInlineBucketSize() {
			return false
		}
	}

	return true
}

// Returns the maximum total size of a bucket to make it a candidate for inlining.
// bucket可以成为内敛桶的最大值
func (b *Bucket) maxInlineBucketSize() uintptr {
	// 当前默认页大小的1/4
	return uintptr(b.tx.db.pageSize / 4)
}

// write allocates and writes a bucket to a byte slice.
// 分配bucket的大小，并将桶写入一个切片
func (b *Bucket) write() []byte {
	// Allocate the appropriate size.
	// 分配合适的大小
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// Write a bucket header.
	// 写入bucket的头信息
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// Convert byte slice to a fake page and write the root node.
	// 字节切片转换为一个假页，并写入到root节点
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// rebalance attempts to balance all nodes.
// 尝试平衡所有的节点信息
func (b *Bucket) rebalance() {
	// 先平衡根桶
	for _, n := range b.nodes {
		n.rebalance()
	}
	// 在平衡子桶
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// node creates a node from a page and associates it with a given parent.
// 根据相关的父节点创建一个页id为pgid的子节点
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// Retrieve node if it's already been created.
	// 如果已经创建了索引节点则返回该节点
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a node and cache it.
	// 否则创建一个节点，如果不存在父节点则将其设置为根节点，否则追加子节点
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// Use the inline page if this is an inline bucket.
	// 如果当前桶是一个内敛桶则使用桶相关的内敛页
	var p = b.page
	if p == nil {
		// 如果没有内敛页，从当期事务中去查找
		p = b.tx.page(pgid)
	}

	// Read the page into the node and cache it.
	// 读取页中的值初始化node并缓存当前节点
	n.read(p)
	b.nodes[pgid] = n

	// Update statistics.
	// 更新事务节点数信息
	b.tx.stats.IncNodeCount(1)

	return n
}

// free recursively frees all pages in the bucket.
// 递归的是否所有桶里面的页
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			// 释放空闲列表
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

// dereference removes all references to the old mmap.
// 删除所有的老的mmap的引用
func (b *Bucket) dereference() {
	// 清空根的内容
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	// 清空子节点的内容
	for _, child := range b.buckets {
		child.dereference()
	}
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
// 如果存在返回内存中的node，否则返回底层的页
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// Inline buckets have a fake page embedded in their value so treat them
	// differently. We'll return the rootNode (if available) or the fake page.
	// 内敛桶有个假页嵌入在值里面需要区别对待，如果rootnode可用我们将返回，或者返回假页
	if b.root == 0 {
		// 内敛桶没有页
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		// 直接返回内敛桶的rootnode
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		// 返回内敛桶底层页
		return b.page, nil
	}

	// Check the node cache for non-inline buckets.
	// 检查非内敛桶的节点缓存，如果存在节点直接返回
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// Finally lookup the page from the transaction if no node is materialized.
	// 最后如果没有节点匹配，则从事务中查找页
	return b.tx.page(id), nil
}

// BucketStats records statistics about resources used by a bucket.
// 桶状态信息
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data

	// Bucket statistics
	BucketN           int // total number of buckets including the top bucket
	InlineBucketN     int // total number on inlined buckets
	InlineBucketInuse int // bytes used for inlined buckets (also accounted for in LeafInuse)
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// cloneBytes returns a copy of a given slice.
// 克隆给定的切片
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
