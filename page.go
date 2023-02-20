package bbolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

// 页头的大小 16个字节
const pageHeaderSize = unsafe.Sizeof(page{})

// 每页的最小key的字节大小
const minKeysPerPage = 2

// 分支节点页中每个元素所占的大小
const branchPageElementSize = unsafe.Sizeof(branchPageElement{})

// 分支节点页中每个元素所占的大小
const leafPageElementSize = unsafe.Sizeof(leafPageElement{})

const (
	// 分支节点页 存储索引信息(页号、元素key值)
	branchPageFlag = 0x01
	// 叶子节点页 存储数据信息(页号、插入的key值、插入的value值)
	leafPageFlag = 0x02
	// 元数据页 存储数据库的元信息，例如空闲列表页id、放置桶的根页等
	metaPageFlag = 0x04
	// 空闲列表页 存储哪些页是空闲页，可以用来后续分配空间时，优先考虑分配
	freelistPageFlag = 0x10
)

const (
	// bucket叶子节点标识
	bucketLeafFlag = 0x01
)

// 页id类型
type pgid uint64

type page struct {
	// 页id 8字节
	id pgid
	// 页类型，可以是分支、叶子节点、元信息、空闲列表 2字节
	flags uint16
	// 个数， 统计叶子节点、非叶子节点、空闲列表页的个数 2字节
	count uint16
	// 数据是否溢出，主要在空闲列表上用 4字节
	overflow uint32
}

// typ returns a human readable page type string used for debugging.
// typ 返回一个用于调试的方便人阅读的page类型字符串
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
// meta 返回指向页面元数据部分的指针。
func (p *page) meta() *meta {
	// 在页中添加一块meta的地址空间，并返回改meta地址
	return (*meta)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
}

// 快速检查，判断页id和页类型标识的合法性
func (p *page) fastCheck(id pgid) {
	_assert(p.id == id, "Page expected to be: %v, but self identifies as %v", id, p.id)
	// Only one flag of page-type can be set.
	_assert(p.flags == branchPageFlag ||
		p.flags == leafPageFlag ||
		p.flags == metaPageFlag ||
		p.flags == freelistPageFlag,
		"page %v: has unexpected type/flags: %x", p.id, p.flags)
}

// leafPageElement retrieves the leaf node by index
// 根据index索引返回叶子页的叶子节点元素
func (p *page) leafPageElement(index uint16) *leafPageElement {
	// 从页头偏移掉16个页头信息，索引出页叶子元素的地址
	return (*leafPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		leafPageElementSize, int(index)))
}

// leafPageElements retrieves a list of leaf nodes.
// 返回叶子页的所有元素
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	var elems []leafPageElement
	// 偏移掉页头信息，获取数据信息地址
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	// 根据页元素个数把数据信息拷贝到leafPageElement切片中
	unsafeSlice(unsafe.Pointer(&elems), data, int(p.count))
	// 返回也元素切片
	return elems
}

// branchPageElement retrieves the branch node by index
// 根据索引返回分支页的节点
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return (*branchPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		unsafe.Sizeof(branchPageElement{}), int(index)))
}

// branchPageElements retrieves a list of branch nodes.
// 返回分支页的所有节点元素
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	var elems []branchPageElement
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	unsafeSlice(unsafe.Pointer(&elems), data, int(p.count))
	return elems
}

// dump writes n bytes of the page to STDERR as hex output.
// 使用十六进制输出页的字节内容到标准错误
func (p *page) hexdump(n int) {
	buf := unsafeByteSlice(unsafe.Pointer(p), 0, 0, n)
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

// 叶子集合类型
type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
// branchPageElement 表示分支页面上的一个节点
type branchPageElement struct {
	pos   uint32 // 元信息与真实key之间的便宜量
	ksize uint32 // key值的大小
	pgid  pgid   // 元素的页id
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	return unsafeByteSlice(unsafe.Pointer(n), 0, int(n.pos), int(n.pos)+int(n.ksize))
}

// leafPageElement represents a node on a leaf page.
// 表示一个叶子节点
type leafPageElement struct {
	//该值主要用来区分，是子桶叶子节点元素还是普通的key/value叶子节点元素。flags值为1时表示子桶。否则为key/value
	flags uint32
	// 元数据与真实数据的偏移量
	pos uint32
	// key 的大小
	ksize uint32
	// value 的大小
	vsize uint32
}

// key returns a byte slice of the node key.
// 返回叶子节点的key
func (n *leafPageElement) key() []byte {
	i := int(n.pos)
	j := i + int(n.ksize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// value returns a byte slice of the node value.
// 返回叶子节点的value
func (n *leafPageElement) value() []byte {
	i := int(n.pos) + int(n.ksize)
	j := i + int(n.vsize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// PageInfo represents human readable information about a page.
// 表示页的描述信息
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

// 实现sort中的排序接口
func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
// 返回a和b合并后的排序pgids
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
// 在dst中排序a和b，如果dst太小，则抛出错误
// 按照从小到大的顺序合并两个排序的pgids
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	// 如果一个切片为空，则dst直接赋值为不为空的一个切片，既返回
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	// 清空dst中的内容
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	// 小的作为lead，开始值较小的赋值给lead，较大的赋值给follow
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		// 从0遍历lead，查找大于follow第一个元素的索引位置
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		// merged缓存中最近lead中0到小于follow中第一个元素的元素
		merged = append(merged, lead[:n]...)
		// n 大于lead，说明已经查找完成
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		// 经过上面的查找，n之后的元素都大于follow的第一个元素，按照规定第一个元素小的为lead，因此对剩下的元素进行交换
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	// 剩下的follow中的元素都大于lead中的最后一个元素，把剩余的follow追加到merged中。
	_ = append(merged, follow...)
}
