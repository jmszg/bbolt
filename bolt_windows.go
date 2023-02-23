package bbolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// fdatasync flushes written data to a file descriptor.
// 刷新文件写入的数据
func fdatasync(db *DB) error {
	//file.Sync() 是一个文件对象的方法，用于将文件缓冲区中的数据写入到磁盘。
	// 具体来说，它会将操作系统缓存中的数据强制刷新到磁盘上，以确保数据的持久性。
	//
	//file.Sync() 函数会阻塞当前协程，直到所有缓冲的数据都被写入到磁盘为止。
	//因此，它通常用于在关闭文件之前，确保所有的数据都被写入到磁盘上。
	//
	//需要注意的是，由于 file.Sync() 函数需要将数据写入磁盘，所以它的执行时间可能比较长，
	//特别是对于大文件或慢速磁盘。因此，建议在必要时使用它，而不是在每个写入操作后都调用它。
	return db.file.Sync()
}

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, exclusive bool, timeout time.Duration) error {
	var t time.Time
	if timeout != 0 {
		t = time.Now()
	}
	var flags uint32 = windows.LOCKFILE_FAIL_IMMEDIATELY
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	for {
		// Fix for https://github.com/etcd-io/bbolt/issues/121. Use byte-range
		// -1..0 as the lock on the database file.
		var m1 uint32 = (1 << 32) - 1 // -1 in a uint32
		//LockFileEx 是 Windows 操作系统提供的函数，用于锁定一个文件或文件的一部分以防止其他进程或线程访问或修改它。这个函数有多个参数，
		//包括文件句柄、锁定的字节数、锁定开始位置等等。具体使用方法可以参考微软官方文档。
		//
		//使用 LockFileEx 锁住文件后，其他进程是无法对该文件或文件的锁定部分进行修改或访问的，
		//直到持有该锁的进程或线程释放该锁。所以，其他进程或线程是不能对被锁定的文件进行读写操作的。
		//但是，如果其他进程或线程尝试打开该文件并请求锁定另一个部分，那么请求可能会成功，因为 LockFileEx 函数仅锁定指定的部分。
		//
		//需要注意的是，锁定文件并不会使其他进程无法访问整个文件，只会防止其他进程对锁定部分进行访问或修改。所以，其他进程仍然可以打开文件并读取未锁定的部分。
		err := windows.LockFileEx(windows.Handle(db.file.Fd()), flags, 0, 1, 0, &windows.Overlapped{
			Offset:     m1,
			OffsetHigh: m1,
		})

		if err == nil {
			return nil
		} else if err != windows.ERROR_LOCK_VIOLATION {
			return err
		}

		// If we timed oumercit then return an error.
		if timeout != 0 && time.Since(t) > timeout-flockRetryTimeout {
			return ErrTimeout
		}

		// Wait for a bit and try again.
		time.Sleep(flockRetryTimeout)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	var m1 uint32 = (1 << 32) - 1 // -1 in a uint32
	return windows.UnlockFileEx(windows.Handle(db.file.Fd()), 0, 1, 0, &windows.Overlapped{
		Offset:     m1,
		OffsetHigh: m1,
	})
}

// mmap memory maps a DB's data file.
// Based on: https://github.com/edsrzf/mmap-go
// 内存映射数据库文件
func mmap(db *DB, sz int) error {
	var sizelo, sizehi uint32

	if !db.readOnly {
		// Truncate the database to the size of the mmap.
		// 根据mmap的大小节点数据库文件
		if err := db.file.Truncate(int64(sz)); err != nil {
			return fmt.Errorf("truncate: %s", err)
		}
		sizehi = uint32(sz >> 32)
		sizelo = uint32(sz) & 0xffffffff
	}

	// Open a file mapping handle.
	// 创建一个文件映射内核对象
	h, errno := syscall.CreateFileMapping(syscall.Handle(db.file.Fd()), nil, syscall.PAGE_READONLY, sizehi, sizelo, nil)
	if h == 0 {
		return os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	// 对文件进行映射
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, 0)
	if addr == 0 {
		// Do our best and report error returned from MapViewOfFile.
		_ = syscall.CloseHandle(h)
		return os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	// 关闭映射句柄
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return os.NewSyscallError("CloseHandle", err)
	}

	// Convert to a byte array.
	// 内存映射保存为字节数组
	db.data = ((*[maxMapSize]byte)(unsafe.Pointer(addr)))
	db.datasz = sz

	return nil
}

// munmap unmaps a pointer from a file.
// Based on: https://github.com/edsrzf/mmap-go
// 从文件中取消映射指针
func munmap(db *DB) error {
	if db.data == nil {
		return nil
	}

	// 调用dll库里面的函数
	addr := (uintptr)(unsafe.Pointer(&db.data[0]))
	var err1 error
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		err1 = os.NewSyscallError("UnmapViewOfFile", err)
	}
	db.data = nil
	db.datasz = 0
	return err1
}
