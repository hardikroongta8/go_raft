package utils

import (
	"encoding/binary"
	"errors"
	"net"
)

var (
	ErrConnClosed = errors.New("connection closed")
)

type Reader struct {
	conn net.Conn
}

func NewReader(conn net.Conn) *Reader {
	return &Reader{
		conn: conn,
	}
}

func (p *Reader) Read() ([]byte, error) {
	sizeBytes := make([]byte, 2)
	_, err := p.conn.Read(sizeBytes)
	if err != nil {
		return nil, err
	}
	sizeInt := binary.BigEndian.Uint16(sizeBytes)
	if sizeInt == 0 {
		return nil, ErrConnClosed
	}
	dataBytes := make([]byte, sizeInt)
	_, err = p.conn.Read(dataBytes)
	if err != nil {
		return nil, err
	}
	//if n != int(sizeInt) {
	//	return nil, fmt.Errorf("invalid format of data, n = %d, sizeInt = %d", n, sizeInt)
	//}
	return dataBytes, nil
}
