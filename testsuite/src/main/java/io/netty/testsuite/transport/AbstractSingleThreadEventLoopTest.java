/*
 * Copyright 2019 The Netty Project
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
package io.netty.testsuite.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractSingleThreadEventLoopTest {
    private static final Runnable NOOP = new Runnable() {
        @Override
        public void run() { }
    };

    protected abstract EventLoopGroup newEventLoopGroup();
    protected abstract Channel newChannel();
    protected abstract Class<? extends ServerChannel> serverChannelClass();

    @Test
    public void testChannelsRegistered() throws Exception {
        EventLoopGroup group = newEventLoopGroup();
        final SingleThreadEventLoop loop = (SingleThreadEventLoop) group.next();

        try {
            final Channel ch1 = newChannel();
            final Channel ch2 = newChannel();

            int rc = registeredChannels(loop);
            boolean channelCountSupported = rc != -1;

            if (channelCountSupported) {
                assertEquals(0, registeredChannels(loop));
            }

            assertTrue(loop.register(ch1).syncUninterruptibly().isSuccess());
            assertTrue(loop.register(ch2).syncUninterruptibly().isSuccess());
            if (channelCountSupported) {
                assertEquals(2, registeredChannels(loop));
            }

            assertTrue(ch1.deregister().syncUninterruptibly().isSuccess());
            if (channelCountSupported) {
                assertEquals(1, registeredChannels(loop));
            }
        } finally {
            group.shutdownGracefully();
        }
    }

    // Only reliable if run from event loop
    private static int registeredChannels(final SingleThreadEventLoop loop) throws Exception {
        return loop.submit(new Callable<Integer>() {
            @Override
            public Integer call() {
                return loop.registeredChannels();
            }
        }).get(1, SECONDS);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shutdownBeforeStart() throws Exception {
        EventLoopGroup group = newEventLoopGroup();
        assertFalse(group.awaitTermination(2, MILLISECONDS));
        group.shutdown();
        assertTrue(group.awaitTermination(200, MILLISECONDS));
    }

    @Test
    public void shutdownGracefullyZeroQuietBeforeStart() throws Exception {
        EventLoopGroup group = newEventLoopGroup();
        assertTrue(group.shutdownGracefully(0L, 2L, SECONDS).await(200L));
    }

    // Copied from AbstractEventLoopTest
    @Test(timeout = 5000)
    public void testShutdownGracefullyNoQuietPeriod() throws Exception {
        EventLoopGroup loop = newEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(loop)
                .channel(serverChannelClass())
                .childHandler(new ChannelInboundHandlerAdapter());

        // Not close the Channel to ensure the EventLoop is still shutdown in time.
        ChannelFuture cf = serverChannelClass() == LocalServerChannel.class
                ? b.bind(new LocalAddress("local")) : b.bind(0);
        cf.sync().channel();

        io.netty.util.concurrent.Future<?> f = loop.shutdownGracefully(0, 1, TimeUnit.MINUTES);
        assertTrue(loop.awaitTermination(600, MILLISECONDS));
        assertTrue(f.syncUninterruptibly().isSuccess());
        assertTrue(loop.isShutdown());
        assertTrue(loop.isTerminated());
    }

    @Test
    public void shutdownGracefullyBeforeStart() throws Exception {
        EventLoopGroup group = newEventLoopGroup();
        assertTrue(group.shutdownGracefully(200L, 1000L, MILLISECONDS).await(500L));
    }

    @Test
    public void gracefulShutdownAfterStart() throws Exception {
        EventLoop loop = newEventLoopGroup().next();
        final CountDownLatch latch = new CountDownLatch(1);
        loop.execute(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        });

        // Wait for the event loop thread to start.
        latch.await();

        // Request the event loop thread to stop.
        loop.shutdownGracefully(200L, 3000L, MILLISECONDS);

        // Wait until the event loop is terminated.
        assertTrue(loop.awaitTermination(500L, MILLISECONDS));

        assertRejection(loop);
    }

    @Test
    public void testGracefulShutdownCompletesWithNoEvents() throws InterruptedException {
        EventLoopGroup group = newEventLoopGroup();
        try {
            group.shutdownGracefully(2, 15, SECONDS);
            assertTrue(group.awaitTermination(5, SECONDS));
        } finally {
            group.shutdownGracefully(0, 0, NANOSECONDS);
        }
    }

    @Test
    public void testShutdownCompletesWithNoEvents() throws InterruptedException {
        EventLoopGroup group = newEventLoopGroup();
        try {
            group.shutdownGracefully();
            assertTrue(group.awaitTermination(5, SECONDS));
        } finally {
            group.shutdownGracefully(0, 0, NANOSECONDS);
        }
    }

    @Test
    public void testMultipleWakeupsFromOutsideEventLoopProcessed() throws InterruptedException {
        final EventLoopGroup group = newEventLoopGroup();
        final EventLoop loop = group.next();
        try {
            for (int i = 0; i < 100; ++i) {
                testMultipleWakeupsFromOutsideEventLoopProcessed(loop);
            }
        } finally {
            group.shutdownGracefully(0, 0, NANOSECONDS);
        }
    }

    private static void testMultipleWakeupsFromOutsideEventLoopProcessed(EventLoop loop) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        loop.execute(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, SECONDS));
    }

    @Test
    public void testMultipleTimersOutsideEventLoopProcessed() throws InterruptedException {
        final EventLoopGroup group = newEventLoopGroup();
        final EventLoop loop = group.next();
        try {
            for (int i = 0; i < 100; ++i) {
                testMultipleTimersOutsideEventLoopProcessed(loop);
            }
        } finally {
            group.shutdownGracefully(0, 0, NANOSECONDS);
        }
    }

    private static void testMultipleTimersOutsideEventLoopProcessed(EventLoop loop) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        loop.schedule(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        }, 10, MILLISECONDS);

        assertTrue(latch.await(5, SECONDS));
    }

    @Test
    public void testSingleTimerOutsideEventLoopZeroDelayExecutes() throws InterruptedException {
        final EventLoopGroup group = newEventLoopGroup();
        final EventLoop loop = group.next();
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            loop.schedule(new Runnable() {
                @Override
                public void run() {
                    latch.countDown();
                }
            }, 0, NANOSECONDS);
            assertTrue(latch.await(5, SECONDS));
        } finally {
            group.shutdownGracefully(0, 0, NANOSECONDS);
        }
    }

    @Test
    public void testZeroTimerInsideEventLoopExecutes() throws InterruptedException {
        final EventLoopGroup group = newEventLoopGroup();
        final EventLoop loop = group.next();
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            loop.execute(new Runnable() {
                @Override
                public void run() {
                    loop.schedule(new Runnable() {
                        @Override
                        public void run() {
                            latch.countDown();
                        }
                    }, 0, NANOSECONDS);
                }
            });
            assertTrue(latch.await(5, SECONDS));
        } finally {
            group.shutdownGracefully(0, 0, NANOSECONDS).syncUninterruptibly();
        }
    }

    @Test
    public void testMultipleThreadsConcurrentTimersAllProcessed() throws ExecutionException, InterruptedException {
        final EventLoopGroup group = newEventLoopGroup();
        final EventLoop loop = group.next();
        final int numThreads = 100;
        final int randomSeed = new Random().nextInt();
        final int maxMillis = 100;
        final int numTimersPerThread = 1000;
        final List<Future<?>> executorFutures = new ArrayList<Future<?>>(numThreads);
        final CountDownLatch latch = new CountDownLatch(numThreads * numTimersPerThread);
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            for (int i = 0; i < numThreads; ++i) {
                executorFutures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        Random random = new Random(randomSeed);
                        try {
                            barrier.await();
                        } catch (Throwable cause) {
                            throw new AssertionError(cause);
                        }
                        for (int j = 0; j < numTimersPerThread; ++j) {
                            loop.schedule(new Runnable() {
                                @Override
                                public void run() {
                                    latch.countDown();
                                }
                            }, random.nextInt(maxMillis), MILLISECONDS);
                        }
                    }
                }));
            }

            for (int i = 0; i < numThreads; ++i) {
                executorFutures.get(i).get();
            }
            assertTrue("count: " + latch.getCount() + " randomSeed: " + randomSeed, latch.await(30, SECONDS));
        } finally {
            executorService.shutdown();
            group.shutdownGracefully(0, 0, NANOSECONDS).syncUninterruptibly();
        }
    }

    private static void assertRejection(EventExecutor loop) {
        try {
            loop.execute(NOOP);
            fail("A task must be rejected after shutdown() is called.");
        } catch (RejectedExecutionException e) {
            // Expected
        }
    }
}
