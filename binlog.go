package bolt

import (
	"bufio"
	"fmt"
	"golang.org/x/sys/windows"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
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

//func (b *Binlog) Open(path string) (*os.File, *bufio.Writer, error) {
//	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
//	if err != nil {
//		return nil, nil, err
//	}
//
//	//var ol windows.Overlapped
//	if err := LockFile(file); err != nil {
//		file.Close()
//		return nil, nil, err
//	}
//
//	defer UnlockFile(file)
//
//	b.file = file
//	b.buf = bufio.NewWriter(file)
//	return file, b.buf, nil
//}

func (b *Binlog) open() (*os.File, *bufio.Writer, error) {
	dir := "." // 假设我们的binlog文件都在当前目录
	baseFileName := "boltbin"
	maxSize := int64(1024 * 1024) // 假设我们的最大尺寸限制是1MB

	// 读取目录
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatalf("Failed to read dir: %s", err)
	}

	// 找到最大的序列号的binlog文件
	var maxSeqNum int
	for _, file := range files {
		if strings.HasPrefix(file.Name(), baseFileName) {
			seqNumStr := strings.TrimPrefix(file.Name(), baseFileName+".")
			seqNum, err := strconv.Atoi(seqNumStr)
			if err == nil && seqNum > maxSeqNum {
				maxSeqNum = seqNum
			}
		}
	}

	// 生成文件名
	fileName := fmt.Sprintf("%s.%06d", baseFileName, maxSeqNum)

	// 打开文件，如果文件不存在则创建
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failed to open file: %s", err)
	}

	// 获取文件的大小
	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("Failed to stat file: %s", err)
	}

	// 如果文件大小超过了我们的限制，那么创建一个新的文件
	if fi.Size() > maxSize {
		// 关闭当前的文件
		err = f.Close()
		if err != nil {
			log.Fatalf("Failed to close file: %s", err)
		}

		// 创建一个新的文件
		maxSeqNum++
		if maxSeqNum > 999999 {
			maxSeqNum = 0
		}
		fileName = fmt.Sprintf("%s.%06d", baseFileName, maxSeqNum)
		f, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("Failed to create new file: %s", err)
		}
	}

	if err := LockFile(f); err != nil {
		f.Close()
		return nil, nil, err
	}

	defer UnlockFile(f)

	b.file = f
	b.buf = bufio.NewWriter(f)
	return f, b.buf, nil

	//fmt.Printf("File %s is ready for use\n", fileName)
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
