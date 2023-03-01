/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"reflect"
	"time"

	"github.com/contactless/org.eclipse.paho.mqtt.golang/packets"
	"golang.org/x/net/websocket"
)

const (
	IN_BUF_SIZE = 32768
)

func openConnection(uri *url.URL, tlsc *tls.Config, timeout time.Duration) (net.Conn, error) {
	switch uri.Scheme {
	case "ws":
		conn, err := websocket.Dial(uri.String(), "mqtt", "ws://localhost")
		if err != nil {
			return nil, err
		}
		conn.PayloadType = websocket.BinaryFrame
		return conn, err
	case "wss":
		config, _ := websocket.NewConfig(uri.String(), "ws://localhost")
		config.Protocol = []string{"mqtt"}
		config.TlsConfig = tlsc
		conn, err := websocket.DialConfig(config)
		if err != nil {
			return nil, err
		}
		conn.PayloadType = websocket.BinaryFrame
		return conn, err
	case "tcp":
		conn, err := net.DialTimeout("tcp", uri.Host, timeout)
		if err != nil {
			return nil, err
		}
		return conn, nil
    case "unix":
        conn, err := net.DialTimeout("unix", uri.Host, timeout)
        if err != nil {
            return nil, err
        }
        return conn, nil
	case "ssl":
		fallthrough
	case "tls":
		fallthrough
	case "tcps":
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", uri.Host, tlsc)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	return nil, errors.New("Unknown protocol")
}

var packetsSent = 0
var packetsReceived = 0

func GetStats() (int, int) {
	return packetsSent, packetsReceived
}

// actually read incoming messages off the wire
// send Message object into ibound channel
func incoming(c *Client) {
	defer c.workers.Done()
	var err error
	var cp packets.ControlPacket

	DEBUG.Println(NET, "incoming started")

	reader := bufio.NewReaderSize(c.conn, IN_BUF_SIZE)
	for {
		if cp, err = packets.ReadPacket(reader); err != nil {
			break
		}
		// Make sure the client isn't stopped yet. There still
		// can be some packets in the buffer after c.conn is
		// closed, so don't try to send more than 1 packet to
		// ibound after the connection is closed. A single
		// packet can still pass through if the socket is
		// closed after this select.
		select {
		case <-c.stop:
			DEBUG.Println(NET, "incoming stopped")
			return
		default:
		}
		// Not trying to disconnect, send the error to the errors channel
		if debugActive() {
			DEBUG.Println(NET, "Received Message")
		}
		packetsReceived += 1
		c.ibound <- cp
	}
	// We received an error on read.
	// If disconnect is in progress, swallow error and return
	select {
	case <-c.stop:
		DEBUG.Println(NET, "incoming stopped")
		return
		// Not trying to disconnect, send the error to the errors channel
	default:
		ERROR.Println(NET, "incoming stopped with error")
		c.errors <- err
		return
	}
}

// receive a Message object on obound, and then
// actually send outgoing message to the wire
func outgoing(c *Client) {
	defer c.workers.Done()
	DEBUG.Println(NET, "outgoing started")

	writer := bufio.NewWriter(c.conn)
	for {
		if debugActive() {
			DEBUG.Println(NET, "outgoing waiting for an outbound message")
		}
		select {
		case <-c.stop:
			DEBUG.Println(NET, "outgoing stopped")
			return
		case pub := <-c.obound:
			msg := pub.p.(*packets.PublishPacket)
			if msg.Qos != 0 && msg.MessageID == 0 {
				msg.MessageID = c.getID(pub.t)
				pub.t.(*PublishToken).messageID = msg.MessageID
			}
			//persist_obound(c.persist, msg)

			if c.options.WriteTimeout > 0 {
				c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteTimeout))
			}

			err := msg.Write(writer)
			if err == nil {
				err = writer.Flush()
			}
			if err != nil {
				ERROR.Println(NET, "outgoing stopped with error")
				c.errors <- err
				msg.Release()
				return
			}

			if c.options.WriteTimeout > 0 {
				// If we successfully wrote, we don't want the timeout to happen during an idle period
				// so we reset it to infinite.
				c.conn.SetWriteDeadline(time.Time{})
			}

			if msg.Qos == 0 {
				pub.t.flowComplete()
			}
			if debugActive() {
				DEBUG.Println(NET, "obound wrote msg, id:", msg.MessageID)
			}
			msg.Release()
			packetsSent += 1
		case msg := <-c.oboundP:
			switch msg.p.(type) {
			case *packets.SubscribePacket:
				msg.p.(*packets.SubscribePacket).MessageID = c.getID(msg.t)
			case *packets.UnsubscribePacket:
				msg.p.(*packets.UnsubscribePacket).MessageID = c.getID(msg.t)
			}
			if debugActive() {
				DEBUG.Println(NET, "obound priority msg to write, type", reflect.TypeOf(msg.p))
			}
			err := msg.p.Write(writer)
			msg.p.Release()
			if err == nil {
				writer.Flush()
			}
			if err != nil {
				ERROR.Println(NET, "outgoing stopped with error")
				c.errors <- err
				return
			}
			switch msg.p.(type) {
			case *packets.DisconnectPacket:
				msg.t.(*DisconnectToken).flowComplete()
				if debugActive() {
					DEBUG.Println(NET, "outbound wrote disconnect, stopping")
				}
				return
			}
			packetsSent += 1
		}
		// Reset ping timer after sending control packet.
		if c.resetPing != nil {
			c.resetPing <- struct{}{}
		}
	}
}

// receive Message objects on ibound
// store messages if necessary
// send replies on obound
// delete messages from store if necessary
func alllogic(c *Client) {

	DEBUG.Println(NET, "logic started")

	for {
		if debugActive() {
			DEBUG.Println(NET, "logic waiting for msg on ibound")
		}

		select {
		case msg := <-c.ibound:
			if debugActive() {
				DEBUG.Println(NET, "logic got msg on ibound")
			}
			//persist_ibound(c.persist, msg)
			switch msg.(type) {
			case *packets.PingrespPacket:
				if debugActive() {
					DEBUG.Println(NET, "received pingresp")
				}
				if c.resetPingResp != nil {
					c.resetPingResp <- struct{}{}
				}
				msg.Release()
			case *packets.SubackPacket:
				sa := msg.(*packets.SubackPacket)
				if debugActive() {
					DEBUG.Println(NET, "received suback, id:", sa.MessageID)
				}
				token := c.getToken(sa.MessageID).(*SubscribeToken)
				if debugActive() {
					DEBUG.Println(NET, "granted qoss", sa.GrantedQoss)
				}
				for i, qos := range sa.GrantedQoss {
					token.subResult[token.subs[i]] = qos
				}
				token.flowComplete()
				go c.freeID(sa.MessageID)
				msg.Release()
			case *packets.UnsubackPacket:
				ua := msg.(*packets.UnsubackPacket)
				if debugActive() {
					DEBUG.Println(NET, "received unsuback, id:", ua.MessageID)
				}
				token := c.getToken(ua.MessageID).(*UnsubscribeToken)
				token.flowComplete()
				go c.freeID(ua.MessageID)
				msg.Release()
			case *packets.PublishPacket:
				pp := msg.(*packets.PublishPacket)
				if debugActive() {
					DEBUG.Println(NET, "received publish, msgId:", pp.MessageID)
					DEBUG.Println(NET, "putting msg on onPubChan")
				}
				switch pp.Qos {
				case 2:
					c.incomingPubChan <- pp
					if debugActive() {
						DEBUG.Println(NET, "done putting msg on incomingPubChan")
					}
					pr := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
					pr.MessageID = pp.MessageID
					if debugActive() {
						DEBUG.Println(NET, "putting pubrec msg on obound")
					}
					c.oboundP <- &PacketAndToken{p: pr, t: nil}
					if debugActive() {
						DEBUG.Println(NET, "done putting pubrec msg on obound")
					}
				case 1:
					c.incomingPubChan <- pp
					if debugActive() {
						DEBUG.Println(NET, "done putting msg on incomingPubChan")
					}
					pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
					pa.MessageID = pp.MessageID
					if debugActive() {
						DEBUG.Println(NET, "putting puback msg on obound")
					}
					c.oboundP <- &PacketAndToken{p: pa, t: nil}
					if debugActive() {
						DEBUG.Println(NET, "done putting puback msg on obound")
					}
				case 0:
					select {
					case c.incomingPubChan <- pp:
						if debugActive() {
							DEBUG.Println(NET, "done putting msg on incomingPubChan")
						}
					case err, ok := <-c.errors:
						if debugActive() {
							DEBUG.Println(NET, "error while putting msg on pubChanZero")
						}
						// We are unblocked, but need to put the error back on so the outer
						// select can handle it appropriately.
						if ok {
							go func(errVal error, errChan chan error) {
								errChan <- errVal
							}(err, c.errors)
						}
					}
				}
				// publish messages aren't released because they are used in another
				// goroutine
			case *packets.PubackPacket:
				pa := msg.(*packets.PubackPacket)
				if debugActive() {
					DEBUG.Println(NET, "received puback, id:", pa.MessageID)
				}
				// c.receipts.get(msg.MsgId()) <- Receipt{}
				// c.receipts.end(msg.MsgId())
				c.getToken(pa.MessageID).flowComplete()
				c.freeID(pa.MessageID)
				msg.Release()
			case *packets.PubrecPacket:
				prec := msg.(*packets.PubrecPacket)
				if debugActive() {
					DEBUG.Println(NET, "received pubrec, id:", prec.MessageID)
				}
				prel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				prel.MessageID = prec.MessageID
				select {
				case c.oboundP <- &PacketAndToken{p: prel, t: nil}:
				case <-time.After(time.Second):
				}
				msg.Release()
			case *packets.PubrelPacket:
				pr := msg.(*packets.PubrelPacket)
				if debugActive() {
					DEBUG.Println(NET, "received pubrel, id:", pr.MessageID)
				}
				pc := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
				pc.MessageID = pr.MessageID
				select {
				case c.oboundP <- &PacketAndToken{p: pc, t: nil}:
				case <-time.After(time.Second):
				}
				msg.Release()
			case *packets.PubcompPacket:
				pc := msg.(*packets.PubcompPacket)
				if debugActive() {
					DEBUG.Println(NET, "received pubcomp, id:", pc.MessageID)
				}
				c.getToken(pc.MessageID).flowComplete()
				c.freeID(pc.MessageID)
				msg.Release()
			}
		case <-c.stop:
			WARN.Println(NET, "logic stopped")
			return
		case err := <-c.errors:
			ERROR.Println(NET, "logic got error")
			c.internalConnLost(err)
			return
		}
	}
}
