package packets

import (
	"bytes"
	"fmt"
)

//SubackPacket is an internal representation of the fields of the
//Suback MQTT packet
type SubackPacket struct {
	*FixedHeader
	MessageID   uint16
	GrantedQoss []byte
}

func (sa *SubackPacket) String() string {
	str := fmt.Sprintf("%s\n", sa.FixedHeader)
	str += fmt.Sprintf("MessageID: %d", sa.MessageID)
	return str
}

func (sa *SubackPacket) Write(w PacketWriter) error {
	var body bytes.Buffer
	var err error
	body.Write(encodeUint16(sa.MessageID))
	body.Write(sa.GrantedQoss)
	sa.FixedHeader.RemainingLength = body.Len()
	packet := sa.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (sa *SubackPacket) Unpack(src []byte) {
	sa.MessageID = loadUint16(src)
	if len(src) < 2 {
		sa.GrantedQoss = make([]byte, 0) // FIXME: error
	} else {
		sa.GrantedQoss = src[2:]
	}
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (sa *SubackPacket) Details() Details {
	return Details{Qos: 0, MessageID: sa.MessageID}
}
