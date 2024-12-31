package utils

import (
	"encoding/binary"
	"net"
	"time"
)

type Writer struct {
	conn net.Conn
}

func NewWriter(conn net.Conn) *Writer {
	return &Writer{
		conn: conn,
	}
}

func (w *Writer) Write(data []byte) error {
	lenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBytes, uint16(len(data)))
	_, err := w.conn.Write(lenBytes)
	if err != nil {
		return err
	}
	_, err = w.conn.Write(data)
	time.Sleep(time.Second)
	return err
}
