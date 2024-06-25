package bolt

import (
	"bufio"
	"golang.org/x/sys/windows"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// binlog struct.
type Binlog struct {
	file *os.File
	time time.Time //binlog time
	op   []string  // "put" or "delete"
	key  []byte    // key
	val  []byte    // value
	LSN  uint64
	buf  *bufio.Writer
}

const (
	LOCKFILE_EXCLUSIVE_LOCK   = 2
	LOCKFILE_FAIL_IMMEDIATELY = 1
)

func LockFile(file *os.File) error {
	var ol windows.Overlapped
	handle := windows.Handle(file.Fd())
	return lockFileEx(syscall.Handle(handle), LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, (*syscall.Overlapped)(unsafe.Pointer(&ol)))
}

func UnlockFile(file *os.File) error {
	var ol windows.Overlapped
	handle := windows.Handle(file.Fd())
	return unlockFileEx(syscall.Handle(handle), 0, 1, 0, (*syscall.Overlapped)(unsafe.Pointer(&ol)))
}

func (b *Binlog) Open(path string) (*os.File, *bufio.Writer, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	//var ol windows.Overlapped
	if err := LockFile(file); err != nil {
		file.Close()
		return nil, nil, err
	}

	defer UnlockFile(file)

	b.file = file
	b.buf = bufio.NewWriter(file)
	return file, b.buf, nil
}

func (b *Binlog) Write(p []byte) (n int, err error) {
	b.LSN++

	// 使用 *bufio.Writer 写入数据
	n, err = b.buf.Write(p)
	if err != nil {
		return n, err
	}

	// 确保数据被写入硬盘
	err = b.buf.Flush()
	if err != nil {
		return n, err
	}

	return n, nil
}
