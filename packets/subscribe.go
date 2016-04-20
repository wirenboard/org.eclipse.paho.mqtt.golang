package packets

import (
	"bytes"
	"fmt"
)

//SubscribePacket is an internal representation of the fields of the
//Subscribe MQTT packet
type SubscribePacket struct {
	*FixedHeader
	MessageID uint16
	Topics    []string
	Qoss      []byte
}

func (s *SubscribePacket) String() string {
	str := fmt.Sprintf("%s", s.FixedHeader)
	str += fmt.Sprintf("MessageID: %d topics: %s", s.MessageID, s.Topics)
	return str
}

func (s *SubscribePacket) Write(w PacketWriter) error {
	var body bytes.Buffer
	var err error

	body.Write(encodeUint16(s.MessageID))
	for i, topic := range s.Topics {
		body.Write(encodeString(topic))
		body.WriteByte(s.Qoss[i])
	}
	s.FixedHeader.RemainingLength = body.Len()
	packet := s.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (s *SubscribePacket) Unpack(src []byte) {
	s.MessageID = loadUint16(src)
	if len(src) < 2 {
		return // FIXME: error
	}
	src = src[2:]
	for len(src) > 2 {
		topic, end := loadString(src)
		src = src[end:]
		if len(src) < 1 {
			break // FIXME: error
		}
		s.Topics = append(s.Topics, topic)
		qos := loadByte(src)
		src = src[1:]
		s.Qoss = append(s.Qoss, qos)
	}
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (s *SubscribePacket) Details() Details {
	return Details{Qos: 1, MessageID: s.MessageID}
}
