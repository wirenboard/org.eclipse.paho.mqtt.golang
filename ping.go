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
	"errors"
	"time"

	"github.com/contactless/org.eclipse.paho.mqtt.golang/packets"
)

func keepalive(c *Client) {
	pingTimer := time.NewTimer(c.options.KeepAlive)
	pingRespTimer := time.NewTimer(time.Duration(10) * time.Second)
	pingRespTimer.Stop()
	DEBUG.Println(PNG, "keepalive starting")

	for {
		select {
		case <-c.stop:
			DEBUG.Println(PNG, "keepalive stopped")
			pingTimer.Stop()
			pingRespTimer.Stop()
			c.workers.Done()
			return
		case <-c.resetPingResp:
			pingRespTimer.Stop()
			pingTimer.Reset(c.options.PingTimeout)
		case <-c.resetPing:
			pingTimer.Reset(c.options.PingTimeout)
		case <-pingTimer.C:
			DEBUG.Println(PNG, "keepalive sending ping")
			ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
			//We don't want to wait behind large messages being sent, the Write call
			//will block until it it able to send the packet.
			w := bufio.NewWriter(c.conn)
			ping.Write(w)
			w.Flush()
			pingRespTimer.Reset(c.options.PingTimeout)
		case <-pingRespTimer.C:
			CRITICAL.Println(PNG, "pingresp not received, disconnecting")
			pingTimer.Stop()
			c.workers.Done()
			c.internalConnLost(errors.New("pingresp not received, disconnecting"))
			return
		}
	}
}
