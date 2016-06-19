// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

var (
	ErrBadRespCRLFEnd  = errors.New("bad resp CRLF end")
	ErrBadRespBytesLen = errors.New("bad resp bytes len")
	ErrBadRespArrayLen = errors.New("bad resp array len")
)

func btoi(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}

	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, errors.Trace(err)
	} else {
		return n, nil
	}
}

//解码结构体
type Decoder struct {
	*bufio.Reader
	Err error
}

//构造函数
func NewDecoder(br *bufio.Reader) *Decoder {
	return &Decoder{Reader: br}
}

func NewDecoderSize(r io.Reader, size int) *Decoder {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, size)
	}
	return &Decoder{Reader: br}
}

//解码函数
func (d *Decoder) Decode() (*Resp, error) {
	if d.Err != nil {
		return nil, d.Err
	}
	r, err := d.decodeResp(0)
	if err != nil {
		d.Err = err
	}
	return r, err
}

//从bufio.Reader读取数据
func Decode(br *bufio.Reader) (*Resp, error) {
	return NewDecoder(br).Decode()
}

//从字符串数组中读取数据，将字符串数组转换为bufio.Reader
func DecodeFromBytes(p []byte) (*Resp, error) {
	return Decode(bufio.NewReader(bytes.NewReader(p)))
}

//解码函数
func (d *Decoder) decodeResp(depth int) (*Resp, error) {
	b, err := d.ReadByte()
	if err != nil {
		return nil, errors.Trace(err)
	}
	//TypeString    RespType = '+'
	//TypeError     RespType = '-'
	//TypeInt       RespType = ':'
	//TypeBulkBytes RespType = '$'
	//TypeArray     RespType = '*'
	//Redis的通讯协议可以说大集汇了……消息头标识，消息行还有就行里可能还有个数据块大小描述.首先Redis是以行来划分，
	//每行以\r\n行结束。每一行都有一个消息头，消息头共分为5种分别如下:
	//	(+) 表示一个正确的状态信息，具体信息是当前行+后面的字符。
	//	(-) 表示一个错误信息，具体信息是当前行－后面的字符。
	//	(*) 表示消息体总共有多少行，不包括当前行,*后面是具体的行数。
	//	($) 表示下一行数据长度，不包括换行符长度\r\n,$后面则是对应的长度的数据。
	//	(:) 表示返回一个数值，：后面是相应的数字节符。
	switch t := RespType(b); t {
	case TypeString, TypeError, TypeInt:
		r := &Resp{Type: t}
		r.Value, err = d.decodeTextBytes()
		return r, err
	case TypeBulkBytes:
		r := &Resp{Type: t}
		r.Value, err = d.decodeBulkBytes()
		return r, err
	case TypeArray:
		r := &Resp{Type: t}
		r.Array, err = d.decodeArray(depth)
		return r, err
	default:
		if depth != 0 {
			return nil, errors.Errorf("bad resp type %s", t)
		}
		if err := d.UnreadByte(); err != nil {
			return nil, errors.Trace(err)
		}
		r := &Resp{Type: TypeArray}
		r.Array, err = d.decodeSingleLineBulkBytesArray()
		return r, err
	}
}

func (d *Decoder) decodeTextBytes() ([]byte, error) {
	b, err := d.ReadBytes('\n')
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	} else {
		return b[:n], nil
	}
}

func (d *Decoder) decodeTextString() (string, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (d *Decoder) decodeInt() (int64, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return 0, err
	}
	return btoi(b)
}

func (d *Decoder) decodeBulkBytes() ([]byte, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, errors.Trace(ErrBadRespBytesLen)
	} else if n == -1 {
		return nil, nil
	}
	b := make([]byte, n+2)
	if _, err := io.ReadFull(d.Reader, b); err != nil {
		return nil, errors.Trace(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	}
	return b[:n], nil
}

func (d *Decoder) decodeArray(depth int) ([]*Resp, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, errors.Trace(ErrBadRespArrayLen)
	} else if n == -1 {
		return nil, nil
	}
	a := make([]*Resp, n)
	for i := 0; i < len(a); i++ {
		if a[i], err = d.decodeResp(depth + 1); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (d *Decoder) decodeSingleLineBulkBytesArray() ([]*Resp, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return nil, err
	}
	a := make([]*Resp, 0, 4)
	for l, r := 0, 0; r <= len(b); r++ {
		if r == len(b) || b[r] == ' ' {
			if l < r {
				a = append(a, &Resp{
					Type:  TypeBulkBytes,
					Value: b[l:r],
				})
			}
			l = r + 1
		}
	}
	return a, nil
}
