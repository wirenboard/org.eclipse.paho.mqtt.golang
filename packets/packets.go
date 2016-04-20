package packets

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

type PacketWriter interface {
	io.Writer
	io.ByteWriter
}

type PacketReader interface {
	io.Reader
	io.ByteReader
}

//ControlPacket defines the interface for structs intended to hold
//decoded MQTT packets, either from being read or before being
//written
type ControlPacket interface {
	Write(PacketWriter) error
	Unpack([]byte)
	String() string
	Details() Details
	Release()
	getByteSlice(int) []byte
}

//PacketNames maps the constants for each of the MQTT packet types
//to a string representation of their name.
var PacketNames = map[uint8]string{
	1:  "CONNECT",
	2:  "CONNACK",
	3:  "PUBLISH",
	4:  "PUBACK",
	5:  "PUBREC",
	6:  "PUBREL",
	7:  "PUBCOMP",
	8:  "SUBSCRIBE",
	9:  "SUBACK",
	10: "UNSUBSCRIBE",
	11: "UNSUBACK",
	12: "PINGREQ",
	13: "PINGRESP",
	14: "DISCONNECT",
}

//Below are the constants assigned to each of the MQTT packet types
const (
	Connect        = 1
	Connack        = 2
	Publish        = 3
	Puback         = 4
	Pubrec         = 5
	Pubrel         = 6
	Pubcomp        = 7
	Subscribe      = 8
	Suback         = 9
	Unsubscribe    = 10
	Unsuback       = 11
	Pingreq        = 12
	Pingresp       = 13
	Disconnect     = 14
	MaxMessageType = 14
)

//Below are the const definitions for error codes returned by
//Connect()
const (
	Accepted                        = 0x00
	ErrRefusedBadProtocolVersion    = 0x01
	ErrRefusedIDRejected            = 0x02
	ErrRefusedServerUnavailable     = 0x03
	ErrRefusedBadUsernameOrPassword = 0x04
	ErrRefusedNotAuthorised         = 0x05
	ErrNetworkError                 = 0xFE
	ErrProtocolViolation            = 0xFF
)

//ConnackReturnCodes is a map of the error codes constants for Connect()
//to a string representation of the error
var ConnackReturnCodes = map[uint8]string{
	0:   "Connection Accepted",
	1:   "Connection Refused: Bad Protocol Version",
	2:   "Connection Refused: Client Identifier Rejected",
	3:   "Connection Refused: Server Unavailable",
	4:   "Connection Refused: Username or Password in unknown format",
	5:   "Connection Refused: Not Authorised",
	254: "Connection Error",
	255: "Connection Refused: Protocol Violation",
}

//ConnErrors is a map of the errors codes constants for Connect()
//to a Go error
var ConnErrors = map[byte]error{
	Accepted:                        nil,
	ErrRefusedBadProtocolVersion:    errors.New("Unnacceptable protocol version"),
	ErrRefusedIDRejected:            errors.New("Identifier rejected"),
	ErrRefusedServerUnavailable:     errors.New("Server Unavailable"),
	ErrRefusedBadUsernameOrPassword: errors.New("Bad user name or password"),
	ErrRefusedNotAuthorised:         errors.New("Not Authorized"),
	ErrNetworkError:                 errors.New("Network Error"),
	ErrProtocolViolation:            errors.New("Protocol Violation"),
}

var fixedHeaderPool = sync.Pool{
	New: func() interface{} { return &FixedHeader{} },
}
var packetPools [MaxMessageType - 1]sync.Pool

//ReadPacket takes an instance of an PacketReader (such as bufio.Reader) and attempts
//to read an MQTT packet from the stream. It returns a ControlPacket
//representing the decoded MQTT packet and an error. One of these returns will
//always be nil, a nil ControlPacket indicating an error occurred.
func ReadPacket(r PacketReader) (cp ControlPacket, err error) {
	fh := fixedHeaderPool.Get().(*FixedHeader)

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fh.unpack(b, r)
	cp = NewControlPacketWithHeader(fh)
	if cp == nil {
		return nil, errors.New("Bad data from client")
	}
	packetBytes := cp.getByteSlice(fh.RemainingLength)
	_, err = io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, err
	}
	cp.Unpack(packetBytes)
	return cp, nil
}

//NewControlPacket is used to create a new ControlPacket of the type specified
//by packetType, this is usually done by reference to the packet type constants
//defined in packets.go. The newly created ControlPacket is empty and a pointer
//is returned.
func NewControlPacket(packetType byte) (cp ControlPacket) {
	switch packetType {
	case Connect:
		cp = &ConnectPacket{FixedHeader: &FixedHeader{MessageType: Connect}}
	case Connack:
		cp = &ConnackPacket{FixedHeader: &FixedHeader{MessageType: Connack}}
	case Disconnect:
		cp = &DisconnectPacket{FixedHeader: &FixedHeader{MessageType: Disconnect}}
	case Publish:
		cp = &PublishPacket{FixedHeader: &FixedHeader{MessageType: Publish}}
	case Puback:
		cp = &PubackPacket{FixedHeader: &FixedHeader{MessageType: Puback}}
	case Pubrec:
		cp = &PubrecPacket{FixedHeader: &FixedHeader{MessageType: Pubrec}}
	case Pubrel:
		cp = &PubrelPacket{FixedHeader: &FixedHeader{MessageType: Pubrel, Qos: 1}}
	case Pubcomp:
		cp = &PubcompPacket{FixedHeader: &FixedHeader{MessageType: Pubcomp}}
	case Subscribe:
		cp = &SubscribePacket{FixedHeader: &FixedHeader{MessageType: Subscribe, Qos: 1}}
	case Suback:
		cp = &SubackPacket{FixedHeader: &FixedHeader{MessageType: Suback}}
	case Unsubscribe:
		cp = &UnsubscribePacket{FixedHeader: &FixedHeader{MessageType: Unsubscribe, Qos: 1}}
	case Unsuback:
		cp = &UnsubackPacket{FixedHeader: &FixedHeader{MessageType: Unsuback}}
	case Pingreq:
		cp = &PingreqPacket{FixedHeader: &FixedHeader{MessageType: Pingreq}}
	case Pingresp:
		cp = &PingrespPacket{FixedHeader: &FixedHeader{MessageType: Pingresp}}
	default:
		return nil
	}
	return cp
}

//NewControlPacketWithHeader is used to create a new ControlPacket of the type
//specified within the FixedHeader that is passed to the function.
//The newly created ControlPacket is empty and a pointer is returned.
func NewControlPacketWithHeader(fh *FixedHeader) (cp ControlPacket) {
	if fh.MessageType == 0 && fh.MessageType > MaxMessageType {
		return nil
	}
	pooled := packetPools[fh.MessageType-1].Get()
	if pooled == nil {
		cp := newControlPacketWithHeader(fh)
		fh.selfPtr = cp
		return cp
	}

	switch fh.MessageType {
	case Connect:
		cp := pooled.(*ConnectPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Connack:
		cp := pooled.(*ConnackPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Disconnect:
		cp := pooled.(*DisconnectPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Publish:
		cp := pooled.(*PublishPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Puback:
		cp := pooled.(*PubackPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Pubrec:
		cp := pooled.(*PubrecPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Pubrel:
		cp := pooled.(*PubrelPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Pubcomp:
		cp := pooled.(*PubcompPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Subscribe:
		cp := pooled.(*SubscribePacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Suback:
		cp := pooled.(*SubackPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Unsubscribe:
		cp := pooled.(*UnsubscribePacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Unsuback:
		cp := pooled.(*UnsubackPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Pingreq:
		cp := pooled.(*PingreqPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	case Pingresp:
		cp := pooled.(*PingrespPacket)
		cp.FixedHeader = fh
		fh.selfPtr = cp
		return cp
	default:
		return nil
	}
}

func newControlPacketWithHeader(fh *FixedHeader) (cp ControlPacket) {
	switch fh.MessageType {
	case Connect:
		cp = &ConnectPacket{FixedHeader: fh}
	case Connack:
		cp = &ConnackPacket{FixedHeader: fh}
	case Disconnect:
		cp = &DisconnectPacket{FixedHeader: fh}
	case Publish:
		cp = &PublishPacket{FixedHeader: fh}
	case Puback:
		cp = &PubackPacket{FixedHeader: fh}
	case Pubrec:
		cp = &PubrecPacket{FixedHeader: fh}
	case Pubrel:
		cp = &PubrelPacket{FixedHeader: fh}
	case Pubcomp:
		cp = &PubcompPacket{FixedHeader: fh}
	case Subscribe:
		cp = &SubscribePacket{FixedHeader: fh}
	case Suback:
		cp = &SubackPacket{FixedHeader: fh}
	case Unsubscribe:
		cp = &UnsubscribePacket{FixedHeader: fh}
	case Unsuback:
		cp = &UnsubackPacket{FixedHeader: fh}
	case Pingreq:
		cp = &PingreqPacket{FixedHeader: fh}
	case Pingresp:
		cp = &PingrespPacket{FixedHeader: fh}
	default:
		return nil
	}
	return cp
}

//Details struct returned by the Details() function called on
//ControlPackets to present details of the Qos and MessageID
//of the ControlPacket
type Details struct {
	Qos       byte
	MessageID uint16
}

//FixedHeader is a struct to hold the decoded information from
//the fixed header of an MQTT ControlPacket
type FixedHeader struct {
	ByteSlicePool
	MessageType     byte
	Dup             bool
	Qos             byte
	Retain          bool
	RemainingLength int
	selfPtr         interface{}
}

func (fh *FixedHeader) String() string {
	return fmt.Sprintf("%s: dup: %t qos: %d retain: %t rLength: %d", PacketNames[fh.MessageType], fh.Dup, fh.Qos, fh.Retain, fh.RemainingLength)
}

func (fh *FixedHeader) Release() {
	fh.ByteSlicePool.Release()
	if fh.selfPtr != nil && fh.MessageType > 0 && fh.MessageType <= MaxMessageType {
		packetPools[fh.MessageType-1].Put(fh.selfPtr)
		// only do the following for incoming packets
		// (created using NewControlPacketWithHeader)
		fixedHeaderPool.Put(fh)
	}
}

func boolToByte(b bool) byte {
	switch b {
	case true:
		return 1
	default:
		return 0
	}
}

func (fh *FixedHeader) pack() bytes.Buffer {
	var header bytes.Buffer
	header.WriteByte(fh.MessageType<<4 | boolToByte(fh.Dup)<<3 | fh.Qos<<1 | boolToByte(fh.Retain))
	header.Write(encodeLength(fh.RemainingLength))
	return header
}

func (fh *FixedHeader) unpack(typeAndFlags byte, r PacketReader) {
	fh.MessageType = typeAndFlags >> 4
	fh.Dup = (typeAndFlags>>3)&0x01 > 0
	fh.Qos = (typeAndFlags >> 1) & 0x03
	fh.Retain = typeAndFlags&0x01 > 0
	fh.RemainingLength = decodeLength(r)
}

func decodeByte(b PacketReader) byte {
	if r, err := b.ReadByte(); err != nil {
		panic("decodeByte() failed")
	} else {
		return r
	}
}

func decodeUint16(b PacketReader) uint16 {
	hi := decodeByte(b)
	lo := decodeByte(b)
	return (uint16(hi) << 8) + uint16(lo)
}

func encodeUint16(num uint16) []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, num)
	return bytes
}

func encodeString(field string) []byte {
	fieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldLength, uint16(len(field)))
	return append(fieldLength, []byte(field)...)
}

// TBD: rm
func decodeString(b PacketReader) string {
	fieldLength := decodeUint16(b)
	field := make([]byte, fieldLength)
	b.Read(field)
	return string(field)
}

func decodeBytes(b PacketReader) []byte {
	fieldLength := decodeUint16(b)
	field := make([]byte, fieldLength)
	b.Read(field)
	return field
}

// TBD: rm
func encodeBytes(field []byte) []byte {
	fieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldLength, uint16(len(field)))
	return append(fieldLength, field...)
}

func encodeLength(length int) []byte {
	var encLength []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		encLength = append(encLength, digit)
		if length == 0 {
			break
		}
	}
	return encLength
}

func decodeLength(r PacketReader) int {
	var rLength uint32
	var multiplier uint32
	for {
		digit, err := r.ReadByte()
		if err != nil {
			panic("decodeLength() failed")
		}
		rLength |= uint32(digit&127) << multiplier
		if (digit & 128) == 0 {
			break
		}
		multiplier += 7
	}
	return int(rLength)
}

// pooled & direct write fns (TBD: use just them) [RM]

func (pool *ByteSlicePool) decodeBytes(b PacketReader) []byte {
	fieldLength := decodeUint16(b)
	field := pool.getByteSlice(int(fieldLength))
	b.Read(field)
	return field
}

func (pool *ByteSlicePool) encodeBytes(field []byte) []byte {
	out := pool.getByteSlice(len(field) + 2)
	binary.BigEndian.PutUint16(out, uint16(len(field)))
	copy(out[2:], field)
	return out
}

func loadByte(src []byte) byte {
	if len(src) == 0 {
		return 0 // FIXME: error
	}
	return src[0]
}

func loadUint16(src []byte) uint16 {
	if len(src) < 2 {
		return 0 // FIXME: error
	}
	hi := src[0]
	lo := src[1]
	return (uint16(hi) << 8) + uint16(lo)
}

func loadBytes(src []byte) ([]byte, int) {
	end := int(loadUint16(src) + 2)
	if len(src) < end {
		return make([]byte, 0), len(src) // FIXME: error
	}
	return src[2:end], end
}

func loadString(src []byte) (string, int) {
	bs, end := loadBytes(src)
	return string(bs), end
}
