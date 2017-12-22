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

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.EmptyQueue;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class CopyOnWriteChannelPipeline implements ChannelPipeline {
    private static final InternalLogger logger =
            InternalLoggerFactory.getInstance(CopyOnWriteChannelPipeline.class);
    private static final AtomicReferenceFieldUpdater<CopyOnWriteChannelPipeline, CopyOnWriteChannelHandlerContext[]>
            pipelineUpdater = AtomicReferenceFieldUpdater.newUpdater(CopyOnWriteChannelPipeline.class,
            CopyOnWriteChannelHandlerContext[].class, "pipelineArray");
    private static final AtomicReferenceFieldUpdater<CopyOnWriteChannelPipeline, Queue>
            pendingCallbackQueueUpdater = AtomicReferenceFieldUpdater.newUpdater(CopyOnWriteChannelPipeline.class,
            Queue.class, "pendingCallbackQueue");
    private static final AtomicReferenceFieldUpdater<CopyOnWriteChannelPipeline, MessageSizeEstimator.Handle>
            estimatorHandleUpdater = AtomicReferenceFieldUpdater.newUpdater(CopyOnWriteChannelPipeline.class,
            MessageSizeEstimator.Handle.class, "estimatorHandle");
    private static final Queue<Runnable> EMPTY_QUEUE = EmptyQueue.instance();
    private static final String HEAD_NAME = generateName0(HeadContext.class);
    private static final String TAIL_NAME = generateName0(TailContext.class);
    private static final FastThreadLocal<Map<Class<?>, String>> nameCaches =
            new FastThreadLocal<Map<Class<?>, String>>() {
                @Override
                protected Map<Class<?>, String> initialValue() throws Exception {
                    return new WeakHashMap<Class<?>, String>();
                }
            };
    volatile CopyOnWriteChannelHandlerContext[] pipelineArray;
    @SuppressWarnings("unused")
    private volatile Queue<Runnable> pendingCallbackQueue;
    @SuppressWarnings("unused")
    private volatile MessageSizeEstimator.Handle estimatorHandle;
    private final Channel channel;
    private final ChannelFuture succeededFuture;
    private final VoidChannelPromise voidPromise;
    private final boolean touch = ResourceLeakDetector.isEnabled();
    private Map<EventExecutorGroup, EventExecutor> childExecutors;

    public CopyOnWriteChannelPipeline(Channel channel) {
        this.channel = ObjectUtil.checkNotNull(channel, "channel");
        this.pipelineArray = newEmptyPipelineArray();
        succeededFuture = new SucceededChannelFuture(channel, null);
        voidPromise =  new VoidChannelPromise(channel, true);
    }

    private CopyOnWriteChannelHandlerContext[] newEmptyPipelineArray() {
        CopyOnWriteChannelHandlerContext[] pipelineArray = new CopyOnWriteChannelHandlerContext[2];
        // head <--> tail
        // inbound ->
        // <- outbound

        // head, the transport
        pipelineArray[0] = new HeadContext(this, pipelineArray, 1, 0);
        // tail, after all the user handlers
        pipelineArray[1] = new TailContext(this, pipelineArray, 1, 0);
        return pipelineArray;
    }

    @Override
    public final CopyOnWriteChannelPipeline addFirst(String name, ChannelHandler handler) {
        return addFirst(null, name, handler);
    }

    @Override
    public final CopyOnWriteChannelPipeline addFirst(EventExecutorGroup group, final String name, ChannelHandler handler) {
        boolean isInbound = isInbound(handler);
        boolean isOutbound = isOutbound(handler);
        EventExecutor executor = childExecutor(group);
        for (;;) {
            CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
            String actualName = name == null ? generateName(pipelineArray, handler)
                                             : checkDuplicateName(pipelineArray, name);
            CopyOnWriteChannelHandlerContext[] newPipelineArray =
                    new CopyOnWriteChannelHandlerContext[pipelineArray.length + 1];
            final int oldLastIndex = pipelineArray.length - 1;

            // Copy the head
            CopyOnWriteChannelHandlerContext oldContext = pipelineArray[0];
            newPipelineArray[0] = new HeadContext(this, newPipelineArray,
                    isInbound ? 1 : oldContext.nextInboundIndex + 1, 0);

            // Insert the new element
            oldContext = pipelineArray[1];
            final CopyOnWriteChannelHandlerContext newContext = newPipelineArray[1] =
                    new DefaultCopyOnWriteChannelHandlerContext(this, newPipelineArray,
                    isInbound(oldContext.handler()) ? 2 : oldContext.nextInboundIndex + 1, 0, executor, actualName,
                    handler);

            // Copy the tail
            oldContext = pipelineArray[oldLastIndex];
            newPipelineArray[pipelineArray.length] = new TailContext(this, newPipelineArray,
                    pipelineArray.length,
                    oldContext.nextOutboundIndex == 0 ? (isOutbound ? 1 : 0) : oldContext.nextOutboundIndex + 1);

            // Copy the middle of the pipeline
            for (int i = 1; i < oldLastIndex; ++i) {
                oldContext = pipelineArray[i];
                newPipelineArray[i + 1] = new DefaultCopyOnWriteChannelHandlerContext(this, newPipelineArray,
                        oldContext.nextInboundIndex + 1,
                        oldContext.nextOutboundIndex == 0 ? (isOutbound ? 1 : 0) : oldContext.nextOutboundIndex + 1,
                        oldContext.executor, oldContext.name(), oldContext.handler());
            }

            if (pipelineUpdater.compareAndSet(this, pipelineArray, newPipelineArray)) {
                invokeHandlerAdded(newContext);
                break;
            }
        }

        return this;
    }

    @Override
    public final ChannelPipeline addLast(String name, ChannelHandler handler) {
        return null;
    }

    @Override
    public final ChannelPipeline addLast(EventExecutorGroup group, String name, ChannelHandler handler) {
        return null;
    }

    @Override
    public final ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler) {
        return null;
    }

    @Override
    public final ChannelPipeline addBefore(EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
        return null;
    }

    @Override
    public final ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler) {
        return null;
    }

    @Override
    public final ChannelPipeline addAfter(EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
        return null;
    }

    public final CopyOnWriteChannelPipeline addFirst(ChannelHandler handler) {
        return addFirst(null, handler);
    }

    @Override
    public final ChannelPipeline addFirst(ChannelHandler... handlers) {
        return handlers.length == 1 ? addFirst(handlers[0]) : addFirst(null, handlers);
    }

    @Override
    public final ChannelPipeline addFirst(EventExecutorGroup group, ChannelHandler... handlers) {
        return null;
    }

    @Override
    public final ChannelPipeline addLast(ChannelHandler... handlers) {
        return null;
    }

    @Override
    public final ChannelPipeline addLast(EventExecutorGroup group, ChannelHandler... handlers) {
        return null;
    }

    private ChannelHandler remove(final CopyOnWriteChannelHandlerContext[] pipelineArray, final int removedI) {
        CopyOnWriteChannelHandlerContext[] newPipelineArray =
                new CopyOnWriteChannelHandlerContext[pipelineArray.length - 1];
        final CopyOnWriteChannelHandlerContext removedContext = pipelineArray[removedI];
        final int newLastIndex = newPipelineArray.length - 1;

        // Copy the head
        CopyOnWriteChannelHandlerContext oldContext = pipelineArray[0];
        newPipelineArray[0] = new HeadContext(this, newPipelineArray,
                oldContext.nextInboundIndex > removedI ? oldContext.nextInboundIndex - 1 :
                oldContext.nextInboundIndex < removedI ? oldContext.nextInboundIndex :
                    removedContext.nextInboundIndex - 1,
                0);

        // Copy the tail
        oldContext = pipelineArray[newPipelineArray.length];
        newPipelineArray[newLastIndex] = new TailContext(this, newPipelineArray,
                newLastIndex,
                oldContext.nextOutboundIndex > removedI ? oldContext.nextOutboundIndex - 1 :
                oldContext.nextOutboundIndex < removedI ? oldContext.nextOutboundIndex :
                    removedContext.nextOutboundIndex);

        // Copy the middle of the pipeline
        int i;
        for (i = 1; i < removedI; ++i) {
            oldContext = pipelineArray[i];
            newPipelineArray[i] = new DefaultCopyOnWriteChannelHandlerContext(this, newPipelineArray,
                    oldContext.nextInboundIndex > removedI ? oldContext.nextInboundIndex - 1 :
                    oldContext.nextInboundIndex < removedI ? oldContext.nextInboundIndex :
                        removedContext.nextInboundIndex - 1,
                    oldContext.nextOutboundIndex,
                    oldContext.executor,
                    oldContext.name(),
                    oldContext.handler());
        }

        for (i = removedI + 1; i <= newLastIndex; ++i) {
            oldContext = pipelineArray[i];
            newPipelineArray[i - 1] = new DefaultCopyOnWriteChannelHandlerContext(this, newPipelineArray,
                    oldContext.nextInboundIndex > removedI ? oldContext.nextInboundIndex - 1 :
                    oldContext.nextInboundIndex < removedI ? oldContext.nextInboundIndex :
                        removedContext.nextInboundIndex - 1,
                    oldContext.nextOutboundIndex > removedI ? oldContext.nextOutboundIndex - 1 :
                    oldContext.nextOutboundIndex < removedI ? oldContext.nextOutboundIndex :
                        removedContext.nextOutboundIndex,
                    oldContext.executor,
                    oldContext.name(),
                    oldContext.handler());
        }

        if (pipelineUpdater.compareAndSet(this, pipelineArray, newPipelineArray)) {
            invokeHandlerRemoved(removedContext);
            return removedContext.handler();
        }
        return null;
    }

    @Override
    public final ChannelPipeline remove(ChannelHandler handler) {
        for (;;) {
            CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
            if (remove(pipelineArray, getContextIndexOrDie(pipelineArray, handler)) != null) {
                break;
            }
        }
        return this;
    }

    @Override
    public final ChannelHandler remove(String name) {
        for (;;) {
            CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
            ChannelHandler handler = remove(pipelineArray, getContextIndexOrDie(pipelineArray, name));
            if (handler != null) {
                return handler;
            }
        }
    }

    @Override
    public final <T extends ChannelHandler> T remove(Class<T> handlerType) {
        for (;;) {
            CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
            ChannelHandler handler = remove(pipelineArray, getContextIndexOrDie(pipelineArray, handlerType));
            if (handler != null) {
                return (T) handler;
            }
        }
    }

    @Override
    public final ChannelHandler removeFirst() {
        return null;
    }

    @Override
    public final ChannelHandler removeLast() {
        return null;
    }

    @Override
    public final ChannelPipeline replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler) {
        return null;
    }

    @Override
    public final ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
        return null;
    }

    @Override
    public final <T extends ChannelHandler> T replace(Class<T> oldHandlerType, String newName,
                                                      ChannelHandler newHandler) {
        return null;
    }

    @Override
    public final ChannelHandler first() {
        return null;
    }

    @Override
    public final ChannelHandlerContext firstContext() {
        return null;
    }

    @Override
    public final ChannelHandler last() {
        return null;
    }

    @Override
    public final ChannelHandlerContext lastContext() {
        return null;
    }

    @Override
    public final ChannelHandler get(String name) {
        return null;
    }

    @Override
    public final <T extends ChannelHandler> T get(Class<T> handlerType) {
        return null;
    }

    @Override
    public final ChannelHandlerContext context(ChannelHandler handler) {
        if (handler == null) {
            throw new NullPointerException("handler");
        }
        return context0(pipelineArray, handler);
    }

    private int getContextIndexOrDie(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                     Class<?> handlerType) {
        int i = contextIndex0(pipelineArray, handlerType);
        if (i < 0) {
            throw new NoSuchElementException(handlerType.getName());
        }
        return i;
    }

    private int getContextIndexOrDie(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                     String name) {
        int i = contextIndex0(pipelineArray, name);
        if (i < 0) {
            throw new NoSuchElementException(name);
        }
        return i;
    }

    private int getContextIndexOrDie(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                     ChannelHandler handler) {
        int i = contextIndex0(pipelineArray, handler);
        if (i < 0) {
            throw new NoSuchElementException(handler.getClass().getName());
        }
        return i;
    }

    final CopyOnWriteChannelHandlerContext context0(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                                    ChannelHandler handler) {
        final int end = pipelineArray.length - 1;
        for (int i = 1; i < end; ++i) {
            CopyOnWriteChannelHandlerContext context = pipelineArray[i];
            if (context.handler() == handler) {
                return context;
            }
        }
        return null;
    }

    final int contextIndex0(CopyOnWriteChannelHandlerContext[] pipelineArray,
                            ChannelHandler handler) {
        final int end = pipelineArray.length - 1;
        for (int i = 1; i < end; ++i) {
            CopyOnWriteChannelHandlerContext context = pipelineArray[i];
            if (context.handler() == handler) {
                return i;
            }
        }
        return -1;
    }

    private int contextIndex0(CopyOnWriteChannelHandlerContext[] pipelineArray,
                              String name) {
        final int end = pipelineArray.length - 1;
        for (int i = 1; i < end; ++i) {
            CopyOnWriteChannelHandlerContext context = pipelineArray[i];
            if (context.name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private int contextIndex0(CopyOnWriteChannelHandlerContext[] pipelineArray,
                              Class<?> handlerType) {
        final int end = pipelineArray.length - 1;
        for (int i = 1; i < end; ++i) {
            CopyOnWriteChannelHandlerContext context = pipelineArray[i];
            if (handlerType.isAssignableFrom(context.handler().getClass())) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public final ChannelHandlerContext context(String name) {
        return null;
    }

    @Override
    public final ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType) {
        return null;
    }

    @Override
    public final Channel channel() {
        return channel;
    }

    @Override
    public final List<String> names() {
        return null;
    }

    @Override
    public final Map<String, ChannelHandler> toMap() {
        return null;
    }

    @Override
    public final ChannelPipeline fireChannelRegistered() {
        CopyOnWriteChannelHandlerContext.invokeChannelRegistered(pipelineArray[0]);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelUnregistered() {
        CopyOnWriteChannelHandlerContext.invokeChannelUnregistered(pipelineArray[0]);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelActive() {
        CopyOnWriteChannelHandlerContext.invokeChannelActive(pipelineArray[0]);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelInactive() {
        CopyOnWriteChannelHandlerContext.invokeChannelInactive(pipelineArray[0]);
        return this;
    }

    @Override
    public final ChannelPipeline fireExceptionCaught(Throwable cause) {
        CopyOnWriteChannelHandlerContext.invokeExceptionCaught(pipelineArray[0], cause);
        return this;
    }

    @Override
    public final ChannelPipeline fireUserEventTriggered(Object event) {
        CopyOnWriteChannelHandlerContext.invokeUserEventTriggered(pipelineArray[0], event);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelRead(Object msg) {
        CopyOnWriteChannelHandlerContext.invokeChannelRead(pipelineArray[0], msg);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelReadComplete() {
        CopyOnWriteChannelHandlerContext.invokeChannelReadComplete(pipelineArray[0]);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelWritabilityChanged() {
        CopyOnWriteChannelHandlerContext.invokeChannelWritabilityChanged(pipelineArray[0]);
        return this;
    }

    @Override
    public final ChannelFuture bind(SocketAddress localAddress) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].bind(localAddress);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].connect(remoteAddress);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].connect(remoteAddress, localAddress);
    }

    @Override
    public final ChannelFuture disconnect() {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].disconnect();
    }

    @Override
    public final ChannelFuture close() {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].close();
    }

    @Override
    public final ChannelFuture deregister() {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].deregister();
    }

    @Override
    public final ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].bind(localAddress, promise);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].connect(remoteAddress, promise);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress,
                                       ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].connect(remoteAddress, localAddress, promise);
    }

    @Override
    public final ChannelFuture disconnect(ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].disconnect(promise);
    }

    @Override
    public final ChannelFuture close(ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].close(promise);
    }

    @Override
    public final ChannelFuture deregister(ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].deregister(promise);
    }

    @Override
    public final ChannelOutboundInvoker read() {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        pipelineArray[pipelineArray.length - 1].read();
        return this;
    }

    @Override
    public final ChannelFuture write(Object msg) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].write(msg);
    }

    @Override
    public final ChannelFuture write(Object msg, ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].write(msg, promise);
    }

    @Override
    public final ChannelPipeline flush() {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        pipelineArray[pipelineArray.length - 1].flush();
        return this;
    }

    @Override
    public final ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].writeAndFlush(msg, promise);
    }

    @Override
    public final ChannelFuture writeAndFlush(Object msg) {
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        return pipelineArray[pipelineArray.length - 1].writeAndFlush(msg);
    }

    @Override
    public final ChannelPromise newPromise() {
        return new DefaultChannelPromise(channel);
    }

    @Override
    public final ChannelProgressivePromise newProgressivePromise() {
        return new DefaultChannelProgressivePromise(channel);
    }

    @Override
    public final ChannelFuture newSucceededFuture() {
        return succeededFuture;
    }

    @Override
    public final ChannelFuture newFailedFuture(Throwable cause) {
        return new FailedChannelFuture(channel, null, cause);
    }

    @Override
    public final ChannelPromise voidPromise() {
        return voidPromise;
    }

    @Override
    public final Iterator<Map.Entry<String, ChannelHandler>> iterator() {
        return toMap().entrySet().iterator();
    }

    /**
     * Returns the {@link String} representation of this pipeline.
     */
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder()
                .append(StringUtil.simpleClassName(this))
                .append('{');
        CopyOnWriteChannelHandlerContext[] pipelineArray = this.pipelineArray;
        final int end = pipelineArray.length - 2;
        if (pipelineArray.length > 2) {
            for (int i = 1; i < end; ++i) {
                CopyOnWriteChannelHandlerContext ctx = pipelineArray[i];
                buf.append('(')
                        .append(ctx.name())
                        .append(" = ")
                        .append(ctx.handler().getClass().getName())
                        .append("), ");
            }

            CopyOnWriteChannelHandlerContext ctx = pipelineArray[end];
            buf.append('(')
                    .append(ctx.name())
                    .append(" = ")
                    .append(ctx.handler().getClass().getName())
                    .append(')');
        }
        buf.append('}');
        return buf.toString();
    }

    /**
     * Called once a {@link Throwable} hit the end of the {@link ChannelPipeline} without been handled by the user
     * in {@link ChannelHandler#exceptionCaught(ChannelHandlerContext, Throwable)}.
     */
    protected void onUnhandledInboundException(Throwable cause) {
        try {
            logger.warn(
                    "An exceptionCaught() event was fired, and it reached at the tail of the pipeline. " +
                            "It usually means the last handler in the pipeline did not handle the exception.",
                    cause);
        } finally {
            ReferenceCountUtil.release(cause);
        }
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelActive(ChannelHandlerContext)}event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledInboundChannelActive() {
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelInactive(ChannelHandlerContext)} event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledInboundChannelInactive() {
    }

    /**
     * Called once a message hit the end of the {@link ChannelPipeline} without been handled by the user
     * in {@link ChannelInboundHandler#channelRead(ChannelHandlerContext, Object)}. This method is responsible
     * to call {@link ReferenceCountUtil#release(Object)} on the given msg at some point.
     */
    protected void onUnhandledInboundMessage(Object msg) {
        try {
            logger.debug(
                    "Discarded inbound message {} that reached at the tail of the pipeline. " +
                            "Please check your pipeline configuration.", msg);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelReadComplete(ChannelHandlerContext)} event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledInboundChannelReadComplete() {
    }

    /**
     * Called once an user event hit the end of the {@link ChannelPipeline} without been handled by the user
     * in {@link ChannelInboundHandler#userEventTriggered(ChannelHandlerContext, Object)}. This method is responsible
     * to call {@link ReferenceCountUtil#release(Object)} on the given event at some point.
     */
    protected void onUnhandledInboundUserEventTriggered(Object evt) {
        // This may not be a configuration error and so don't log anything.
        // The event may be superfluous for the current pipeline configuration.
        ReferenceCountUtil.release(evt);
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelWritabilityChanged(ChannelHandlerContext)} event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledChannelWritabilityChanged() {
    }

    @UnstableApi
    protected void incrementPendingOutboundBytes(long size) {
        ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
        if (buffer != null) {
            buffer.incrementPendingOutboundBytes(size);
        }
    }

    @UnstableApi
    protected void decrementPendingOutboundBytes(long size) {
        ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
        if (buffer != null) {
            buffer.decrementPendingOutboundBytes(size);
        }
    }

    final MessageSizeEstimator.Handle estimatorHandle() {
        MessageSizeEstimator.Handle handle = estimatorHandle;
        return handle == null ? initEstimatorHandle() : handle;
    }

    private MessageSizeEstimator.Handle initEstimatorHandle() {
        MessageSizeEstimator.Handle handle = channel.config().getMessageSizeEstimator().newHandle();
        if (!estimatorHandleUpdater.compareAndSet(this, null, handle)) {
            return estimatorHandle;
        }
        return handle;
    }

    final Object touch(Object msg, CopyOnWriteChannelHandlerContext next) {
        return touch ? ReferenceCountUtil.touch(msg, next) : msg;
    }

    final void destroy() {
        CopyOnWriteChannelHandlerContext[] pipeline = this.pipelineArray;
        destroyTowardTail(pipeline, 1, false);
    }

    private void destroyTowardTail(final CopyOnWriteChannelHandlerContext[] pipelineArray,
                                   int i,
                                   boolean inEventLoop) {
        final Thread currentThread = Thread.currentThread();
        final int end = pipelineArray.length - 1;
        for (; i < end; ++i) {
            final CopyOnWriteChannelHandlerContext ctx = pipelineArray[i];
            final EventExecutor executor = ctx.executor();
            if (!inEventLoop && !executor.inEventLoop(currentThread)) {
                final int finalI = i;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        destroyTowardTail(pipelineArray, finalI + 1, true);
                    }
                });
                break;
            }
            inEventLoop = false;
        }

        final CopyOnWriteChannelHandlerContext[] newEmptyPipelineArray = newEmptyPipelineArray();
        if (pipelineUpdater.compareAndSet(this, pipelineArray, newEmptyPipelineArray)) {
            destroyTowardHead(currentThread, pipelineArray, i, inEventLoop);
        } else {
            destroyTowardTail(this.pipelineArray, 1, false);
        }
    }

    private void destroyTowardHead(final Thread currentThread,
                                   final CopyOnWriteChannelHandlerContext[] pipelineArray,
                                   int i,
                                   boolean inEventLoop) {
        for (; i > 0; --i) {
            final CopyOnWriteChannelHandlerContext ctx = pipelineArray[i];
            final EventExecutor executor = ctx.executor();
            if (inEventLoop || executor.inEventLoop(currentThread)) {
                invokeHandlerRemoved(ctx);
            } else {
                final int finalI = i;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        destroyTowardHead(Thread.currentThread(), pipelineArray, finalI - 1, true);
                    }
                });
                break;
            }
            inEventLoop = false;
        }
    }

    static void drainPendingCallbackQueue(Queue<Runnable> queue) {
        Runnable pendingCallback;
        while ((pendingCallback = queue.poll()) != null) {
            pendingCallback.run();
        }
    }

    private void invokeHandlerAdded(final CopyOnWriteChannelHandlerContext newContext) {
        Queue<Runnable> pendingCallbackQueue = this.pendingCallbackQueue;
        if (pendingCallbackQueue != EMPTY_QUEUE) {
            Queue<Runnable> nextQueue = new ConcurrentLinkedQueue<Runnable>();
            if (pendingCallbackQueueUpdater.compareAndSet(this, null, nextQueue)) {
                pendingCallbackQueue = nextQueue;
            } else {
                pendingCallbackQueue = this.pendingCallbackQueue;
            }
            // We rely upon the EventLoop to provide ordering in case the queue is currently being drained by the
            // EventLoop thread.
            if (pendingCallbackQueue != EMPTY_QUEUE) {
                pendingCallbackQueue.add(new Runnable() {
                    @Override
                    public void run() {
                        newContext.invokeHandlerAdded();
                    }
                });
                pendingCallbackQueue = this.pendingCallbackQueue;
                if (pendingCallbackQueue == EMPTY_QUEUE) {
                    drainPendingCallbackQueue(nextQueue);
                }
            } else {
                newContext.invokeHandlerAdded();
            }
        } else {
            newContext.invokeHandlerAdded();
        }
    }

    private void invokeHandlerRemoved(final CopyOnWriteChannelHandlerContext removedContext) {
        Queue<Runnable> pendingCallbackQueue = this.pendingCallbackQueue;
        if (pendingCallbackQueue != EMPTY_QUEUE) {
            Queue<Runnable> nextQueue = new ConcurrentLinkedQueue<Runnable>();
            if (pendingCallbackQueueUpdater.compareAndSet(this, null, nextQueue)) {
                pendingCallbackQueue = nextQueue;
            } else {
                pendingCallbackQueue = this.pendingCallbackQueue;
            }
            // We rely upon the EventLoop to provide ordering in case the queue is currently being drained by the
            // EventLoop thread.
            if (pendingCallbackQueue != EMPTY_QUEUE) {
                pendingCallbackQueue.add(new Runnable() {
                    @Override
                    public void run() {
                        removedContext.invokeHandlerRemoved();
                    }
                });
                pendingCallbackQueue = this.pendingCallbackQueue;
                if (pendingCallbackQueue == EMPTY_QUEUE) {
                    drainPendingCallbackQueue(nextQueue);
                }
            } else {
                removedContext.invokeHandlerRemoved();
            }
        } else {
            removedContext.invokeHandlerRemoved();
        }
    }

    private String checkDuplicateName(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                    String name) {
        if (context0(pipelineArray, name) != null) {
            throw new IllegalArgumentException("Duplicate handler name: " + name);
        }
        return name;
    }

    private CopyOnWriteChannelHandlerContext context0(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                                      String name) {
        int end = pipelineArray.length - 1;
        for (int i = 1; i < end; ++i) {
            CopyOnWriteChannelHandlerContext context = pipelineArray[i];
            if (context.name().equals(name)) {
                return context;
            }
        }
        return null;
    }

    private String generateName(CopyOnWriteChannelHandlerContext[] pipelineArray,
                                ChannelHandler handler) {
        Map<Class<?>, String> cache = nameCaches.get();
        Class<?> handlerType = handler.getClass();
        String name = cache.get(handlerType);
        if (name == null) {
            name = generateName0(handlerType);
            cache.put(handlerType, name);
        }

        // It's not very likely for a user to put more than one handler of the same type, but make sure to avoid
        // any name conflicts.  Note that we don't cache the names generated here.
        if (context0(pipelineArray, name) != null) {
            String baseName = name.substring(0, name.length() - 1); // Strip the trailing '0'.
            for (int i = 1;; i ++) {
                String newName = baseName + i;
                if (context0(pipelineArray, newName) == null) {
                    name = newName;
                    break;
                }
            }
        }
        return name;
    }

    private EventExecutor childExecutor(EventExecutorGroup group) {
        if (group == null) {
            return null;
        }
        Boolean pinEventExecutor = channel.config().getOption(ChannelOption.SINGLE_EVENTEXECUTOR_PER_GROUP);
        if (pinEventExecutor != null && !pinEventExecutor) {
            return group.next();
        }
        Map<EventExecutorGroup, EventExecutor> childExecutors = this.childExecutors;
        if (childExecutors == null) {
            // Use size of 4 as most people only use one extra EventExecutor.
            childExecutors = this.childExecutors = new IdentityHashMap<EventExecutorGroup, EventExecutor>(4);
        }
        // Pin one of the child executors once and remember it so that the same child executor
        // is used to fire events for the same channel.
        EventExecutor childExecutor = childExecutors.get(group);
        if (childExecutor == null) {
            childExecutor = group.next();
            childExecutors.put(group, childExecutor);
        }
        return childExecutor;
    }

    private static String generateName0(Class<?> handlerType) {
        return StringUtil.simpleClassName(handlerType) + "#0";
    }

    final class DefaultCopyOnWriteChannelHandlerContext extends CopyOnWriteChannelHandlerContext {
        private final ChannelHandler handler;
        DefaultCopyOnWriteChannelHandlerContext(CopyOnWriteChannelPipeline pipeline,
                                                CopyOnWriteChannelHandlerContext[] pipelineArray, int nextInboundIndex,
                                                int nextOutboundIndex, EventExecutor executor, String name,
                                                ChannelHandler handler) {
            super(pipeline, pipelineArray, nextInboundIndex, nextOutboundIndex, executor, name);
            this.handler = handler;
        }

        @Override
        public ChannelHandler handler() {
            return handler;
        }
    }

    private final class HeadContext extends CopyOnWriteChannelHandlerContext
            implements ChannelOutboundHandler, ChannelInboundHandler {
        private final Channel.Unsafe unsafe;
        HeadContext(CopyOnWriteChannelPipeline pipeline, CopyOnWriteChannelHandlerContext[] pipelineArray, int nextInboundIndex, int nextOutboundIndex) {
            super(pipeline, pipelineArray, nextInboundIndex, nextOutboundIndex, null, HEAD_NAME);
            unsafe = pipeline.channel().unsafe();
        }

        @Override
        public ChannelHandler handler() {
            return this;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            Queue<Runnable> queue = pendingCallbackQueueUpdater.getAndSet(CopyOnWriteChannelPipeline.this, EMPTY_QUEUE);
            if (queue != null) {
                drainPendingCallbackQueue(queue);
            }
            ctx.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelUnregistered();

            if (!channel.isOpen()) {
                destroy();
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelActive();
            readIfIsAutoRead();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelInactive();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ctx.fireChannelRead(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelReadComplete();
            readIfIsAutoRead();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise)
                throws Exception {
            unsafe.bind(localAddress, promise);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                            ChannelPromise promise) throws Exception {
            unsafe.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            unsafe.disconnect(promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            unsafe.close(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            unsafe.deregister(promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            unsafe.beginRead();
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            unsafe.write(msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            unsafe.flush();
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.fireExceptionCaught(cause);
        }

        private void readIfIsAutoRead() {
            if (channel.config().isAutoRead()) {
                channel.read();
            }
        }
    }

    final class TailContext extends CopyOnWriteChannelHandlerContext implements ChannelInboundHandler {
        TailContext(CopyOnWriteChannelPipeline pipeline, CopyOnWriteChannelHandlerContext[] pipelineArray, int nextInboundIndex, int nextOutboundIndex) {
            super(pipeline, pipelineArray, nextInboundIndex, nextOutboundIndex, null, TAIL_NAME);
        }

        @Override
        public ChannelHandler handler() {
            return this;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            onUnhandledInboundChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            onUnhandledInboundChannelInactive();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            onUnhandledInboundMessage(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            onUnhandledInboundChannelReadComplete();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            onUnhandledInboundUserEventTriggered(evt);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            onUnhandledChannelWritabilityChanged();
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            onUnhandledInboundException(cause);
        }
    }

    private static boolean isInbound(ChannelHandler handler) {
        return handler instanceof ChannelInboundHandler;
    }

    private static boolean isOutbound(ChannelHandler handler) {
        return handler instanceof ChannelOutboundHandler;
    }
}
