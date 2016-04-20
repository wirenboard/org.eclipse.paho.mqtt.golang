package packets

import (
	"fmt"
)

//PubrecPacket is an internal representation of the fields of the
//Pubrec MQTT packet
type PubrecPacket struct {
	*FixedHeader
	MessageID uint16
}

func (pr *PubrecPacket) String() string {
	str := fmt.Sprintf("%s\n", pr.FixedHeader)
	str += fmt.Sprintf("MessageID: %d", pr.MessageID)
	return str
}

func (pr *PubrecPacket) Write(w PacketWriter) error {
	var err error
	pr.FixedHeader.RemainingLength = 2
	packet := pr.FixedHeader.pack()
	packet.Write(encodeUint16(pr.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pr *PubrecPacket) Unpack(src []byte) {
	pr.MessageID = loadUint16(src)
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (pr *PubrecPacket) Details() Details {
	return Details{Qos: pr.Qos, MessageID: pr.MessageID}
}
