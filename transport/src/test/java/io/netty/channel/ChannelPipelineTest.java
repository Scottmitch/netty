/*
 * Copyright 2017 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public abstract class ChannelPipelineTest {
    private static final Object USER_EVENT = new Object();
    private static final Object READ_MSG = new Object();
    protected abstract ChannelPipeline newPipeline(Channel channel);

    @Test
    public void addFirstInboundHandler() {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPipeline pipeline = newPipeline(channel);
        Queue<ChannelPipelineTestEvent> events = new ArrayDeque<ChannelPipelineTestEvent>();
        TestChannelInboundHandler inHandler1 = new TestChannelInboundHandler(events);
        TestChannelInboundHandler inHandler2 = new TestChannelInboundHandler(events);
        TestChannelOutboundHandler outHandler1 = new TestChannelOutboundHandler(events);
        TestChannelInboundHandler inHandler3 = new TestChannelInboundHandler(events);
        pipeline.addFirst(inHandler1)
                .addFirst(inHandler2)
                .addFirst(outHandler1)
                .addFirst(inHandler3);
        assertTrue(events.isEmpty());

        pipeline.fireChannelRegistered();
        assertEquals(7, events.size());
        events.poll().assertContents(inHandler1, EventType.HANDLER_ADDED);
        events.poll().assertContents(inHandler2, EventType.HANDLER_ADDED);
        events.poll().assertContents(outHandler1, EventType.HANDLER_ADDED);
        events.poll().assertContents(inHandler3, EventType.HANDLER_ADDED);
        events.poll().assertContents(inHandler3, EventType.CHANNEL_REGISTERED);
        events.poll().assertContents(inHandler2, EventType.CHANNEL_REGISTERED);
        events.poll().assertContents(inHandler1, EventType.CHANNEL_REGISTERED);

        pipeline.fireChannelRead(READ_MSG);
        assertEquals(3, events.size());
        events.poll().assertContents(inHandler3, EventType.CHANNEL_READ, READ_MSG);
        events.poll().assertContents(inHandler2, EventType.CHANNEL_READ, READ_MSG);
        events.poll().assertContents(inHandler1, EventType.CHANNEL_READ, READ_MSG);
    }

    @Test
    public void removeInboundHandler() {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPipeline pipeline = newPipeline(channel);
        Queue<ChannelPipelineTestEvent> events = new ArrayDeque<ChannelPipelineTestEvent>();
        TestChannelInboundHandler inHandler1 = new TestChannelInboundHandler(events);
        TestChannelInboundHandler inHandler2 = new TestChannelInboundHandler(events);
        TestChannelOutboundHandler outHandler1 = new TestChannelOutboundHandler(events);
        TestChannelInboundHandler inHandler3 = new TestChannelInboundHandler(events);
        pipeline.addFirst(inHandler1)
                .addFirst(inHandler2)
                .addFirst(outHandler1)
                .addFirst(inHandler3);
        assertTrue(events.isEmpty());

        pipeline.fireChannelRegistered();
        assertEquals(7, events.size());
        events.poll().assertContents(inHandler1, EventType.HANDLER_ADDED);
        events.poll().assertContents(inHandler2, EventType.HANDLER_ADDED);
        events.poll().assertContents(outHandler1, EventType.HANDLER_ADDED);
        events.poll().assertContents(inHandler3, EventType.HANDLER_ADDED);
        events.poll().assertContents(inHandler3, EventType.CHANNEL_REGISTERED);
        events.poll().assertContents(inHandler2, EventType.CHANNEL_REGISTERED);
        events.poll().assertContents(inHandler1, EventType.CHANNEL_REGISTERED);

        pipeline.remove(inHandler2);
        assertEquals(1, events.size());
        events.poll().assertContents(inHandler2, EventType.HANDLER_REMOVED);

        pipeline.fireUserEventTriggered(USER_EVENT);
        assertEquals(2, events.size());
        events.poll().assertContents(inHandler3, EventType.USER_EVENT, USER_EVENT);
        events.poll().assertContents(inHandler1, EventType.USER_EVENT, USER_EVENT);
    }

    private static final class TestChannelInboundHandler extends ChannelInboundHandlerAdapter {
        private final Queue<ChannelPipelineTestEvent> events;

        TestChannelInboundHandler(Queue<ChannelPipelineTestEvent> events) {
            this.events = events;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.CHANNEL_REGISTERED));
            ctx.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.CHANNEL_UNREGISTERED));
            ctx.fireChannelUnregistered();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.CHANNEL_READ, msg));
            ctx.fireChannelRead(msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.USER_EVENT, evt));
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.HANDLER_ADDED));
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.HANDLER_REMOVED));
        }
    }

    private static final class TestChannelOutboundHandler extends ChannelOutboundHandlerAdapter {
        private final Queue<ChannelPipelineTestEvent> events;

        TestChannelOutboundHandler(Queue<ChannelPipelineTestEvent> events) {
            this.events = events;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.HANDLER_ADDED));
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            events.add(new ChannelPipelineTestEvent(this, EventType.HANDLER_REMOVED));
        }
    }

    private static final class ChannelPipelineTestEvent {
        final ChannelHandler handler;
        final EventType type;
        final Object obj;

        ChannelPipelineTestEvent(ChannelHandler handler, EventType type) {
            this(handler, type, null);
        }

        ChannelPipelineTestEvent(ChannelHandler handler, EventType type, Object obj) {
            this.handler = handler;
            this.type = type;
            this.obj = obj;
        }

        void assertContents(ChannelHandler expectedHandler, EventType expectedType) {
            assertSame(expectedHandler, handler);
            assertEquals(expectedType, type);
            assertNull(obj);
        }

        void assertContents(ChannelHandler expectedHandler, EventType expectedType, Object expectedObj) {
            assertSame(expectedHandler, handler);
            assertEquals(expectedType, type);
            assertSame(expectedObj, obj);
        }
    }

    private enum EventType {
        HANDLER_ADDED,
        HANDLER_REMOVED,
        CHANNEL_REGISTERED,
        CHANNEL_UNREGISTERED,
        CHANNEL_READ,
        USER_EVENT
    }
}
