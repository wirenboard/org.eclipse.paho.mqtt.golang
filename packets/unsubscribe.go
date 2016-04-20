package packets

import (
	"bytes"
	"fmt"
)

//UnsubscribePacket is an internal representation of the fields of the
//Unsubscribe MQTT packet
type UnsubscribePacket struct {
	*FixedHeader
	MessageID uint16
	Topics    []string
}

func (u *UnsubscribePacket) String() string {
	str := fmt.Sprintf("%s\n", u.FixedHeader)
	str += fmt.Sprintf("MessageID: %d", u.MessageID)
	return str
}

func (u *UnsubscribePacket) Write(w PacketWriter) error {
	var body bytes.Buffer
	var err error
	body.Write(encodeUint16(u.MessageID))
	for _, topic := range u.Topics {
		body.Write(encodeString(topic))
	}
	u.FixedHeader.RemainingLength = body.Len()
	packet := u.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (u *UnsubscribePacket) Unpack(src []byte) {
	u.MessageID = loadUint16(src)
	var topic string
	var end int
	for topic, end = loadString(src); topic != ""; topic, end = loadString(src) {
		u.Topics = append(u.Topics, topic)
		src = src[end:]
	}
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (u *UnsubscribePacket) Details() Details {
	return Details{Qos: 1, MessageID: u.MessageID}
}
