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
	"container/list"
	"sync"

	"github.com/contactless/org.eclipse.paho.mqtt.golang/packets"
)

// route is a type which associates MQTT Topic strings with a
// callback to be executed upon the arrival of a message associated
// with a subscription to that topic.
type route struct {
	topicBytes []byte
	callback   MessageHandler
}

func routeIncludesTopic(route, topic []byte) bool {
	// return match(splitTopicMemoized(route), splitTopicMemoized(topic))
	lr := len(route)
	lt := len(topic)
	if lr == 0 {
		return lt == 0
	}
	if lt == 0 {
		return len(route) == 1 && route[0] == '#'
	}
	tp := 0
	for rp := 0; rp < lr; rp += 1 {
		if route[rp] == '#' {
			return true
		}
		if route[rp] == '+' {
			for tp < lt && topic[tp] != '/' {
				tp += 1
			}
			continue
		}
		if tp == lt || route[rp] != topic[tp] {
			return false
		}
		tp += 1
	}
	return tp == lt
}

// match takes the topic string of the published message and does a basic compare to the
// string of the current Route, if they match it returns true
func (r *route) match(topic string) bool {
	return string(r.topicBytes) == topic || routeIncludesTopic(r.topicBytes, []byte(topic))
}

func (r *route) matchBytes(topic []byte) bool {
	return routeIncludesTopic(r.topicBytes, topic)
}

type router struct {
	sync.RWMutex
	routes         *list.List
	defaultHandler MessageHandler
	messages       chan *packets.PublishPacket
	stop           chan bool
}

// newRouter returns a new instance of a Router and channel which can be used to tell the Router
// to stop
func newRouter() (*router, chan bool) {
	router := &router{routes: list.New(), messages: make(chan *packets.PublishPacket), stop: make(chan bool)}
	stop := router.stop
	return router, stop
}

// addRoute takes a topic string and MessageHandler callback. It looks in the current list of
// routes to see if there is already a matching Route. If there is it replaces the current
// callback with the new one. If not it add a new entry to the list of Routes.
func (r *router) addRoute(topic string, callback MessageHandler) {
	r.Lock()
	defer r.Unlock()
	for e := r.routes.Front(); e != nil; e = e.Next() {
		if e.Value.(*route).match(topic) {
			r := e.Value.(*route)
			r.callback = callback
			return
		}
	}
	r.routes.PushBack(&route{topicBytes: []byte(topic), callback: callback})
}

// deleteRoute takes a route string, looks for a matching Route in the list of Routes. If
// found it removes the Route from the list.
func (r *router) deleteRoute(topic string) {
	r.Lock()
	defer r.Unlock()
	for e := r.routes.Front(); e != nil; e = e.Next() {
		if e.Value.(*route).match(topic) {
			r.routes.Remove(e)
			return
		}
	}
}

// setDefaultHandler assigns a default callback that will be called if no matching Route
// is found for an incoming Publish.
func (r *router) setDefaultHandler(handler MessageHandler) {
	r.defaultHandler = handler
}

// matchAndDispatch takes a channel of Message pointers as input and starts a go routine that
// takes messages off the channel, matches them against the internal route list and calls the
// associated callback (or the defaultHandler, if one exists and no other route matched). If
// anything is sent down the stop channel the function will end.
func (r *router) matchAndDispatch(messages <-chan *packets.PublishPacket, order bool, client *Client) {
	go func() {
		for {
			select {
			case message := <-messages:
				sent := false
				r.RLock()
				for e := r.routes.Front(); e != nil; e = e.Next() {
					if e.Value.(*route).matchBytes(message.TopicName) {
						if order {
							r.RUnlock()
							e.Value.(*route).callback(client, messageFromPublish(message))
							r.RLock()
						} else {
							go e.Value.(*route).callback(client, messageFromPublish(message))
						}
						sent = true
					}
				}
				r.RUnlock()
				if !sent && r.defaultHandler != nil {
					if order {
						r.RLock()
						r.defaultHandler(client, messageFromPublish(message))
						r.RUnlock()
					} else {
						go r.defaultHandler(client, messageFromPublish(message))
					}
				}
				message.Release()
			case <-r.stop:
				return
			}
		}
	}()
}
