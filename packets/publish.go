package packets

import (
	"bytes"
	"fmt"
	// "log"
)

//PublishPacket is an internal representation of the fields of the
//Publish MQTT packet
type PublishPacket struct {
	*FixedHeader
	TopicName []byte
	MessageID uint16
	Payload   []byte
}

func (p *PublishPacket) String() string {
	str := fmt.Sprintf("%s\n", p.FixedHeader)
	str += fmt.Sprintf("topicName: %s MessageID: %d\n", p.TopicName, p.MessageID)
	str += fmt.Sprintf("payload: %s\n", string(p.Payload))
	return str
}

func (p *PublishPacket) Write(w PacketWriter) error {
	var body bytes.Buffer
	var err error

	body.Write(p.encodeBytes(p.TopicName))
	if p.Qos > 0 {
		body.Write(encodeUint16(p.MessageID))
	}
	p.FixedHeader.RemainingLength = body.Len() + len(p.Payload)
	packet := p.FixedHeader.pack()
	packet.Write(body.Bytes())
	packet.Write(p.Payload)
	_, err = w.Write(packet.Bytes())

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (p *PublishPacket) Unpack(src []byte) {
	var end int
	p.TopicName, end = loadBytes(src)
	src = src[end:]
	if p.Qos > 0 {
		p.MessageID = loadUint16(src)
		src = src[2:]
	}
	p.Payload = src
}

//Copy creates a new PublishPacket with the same topic and payload
//but an empty fixed header, useful for when you want to deliver
//a message with different properties such as Qos but the same
//content
func (p *PublishPacket) Copy() *PublishPacket {
	newP := NewControlPacket(Publish).(*PublishPacket)
	newP.TopicName = p.TopicName
	newP.Payload = p.Payload

	return newP
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (p *PublishPacket) Details() Details {
	return Details{Qos: p.Qos, MessageID: p.MessageID}
}
