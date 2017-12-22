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
package io.netty.microbench.channel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.CopyOnWriteChannelPipeline;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.microbench.util.AbstractMicrobenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Threads(1)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CopyOnWritePipelineReadBenchmark extends AbstractMicrobenchmark {
    private static final Object READ_OBJECT = new Object();
    private ChannelPipeline cowPipeline;
    private ChannelPipeline oldPipeline;
    private MutableInteger cowCount;
    private MutableInteger oldCount;

    @Setup(Level.Trial)
    public void setup() {
        EmbeddedChannel channel = new EmbeddedChannel();
        cowPipeline = new CopyOnWriteChannelPipeline(channel);
        oldPipeline = new DefaultChannelPipeline(channel) { };
        cowCount = new MutableInteger();
        cowPipeline.addFirst(new TestInboundHandler(cowCount, false));
        cowPipeline.addFirst(new TestInboundHandler(cowCount, true));
        cowPipeline.addFirst(new TestOutboundHandler());
        cowPipeline.addFirst(new TestInboundHandler(cowCount, true));
        cowPipeline.fireChannelRegistered();

        oldCount = new MutableInteger();
        oldPipeline.addFirst(new TestInboundHandler(oldCount, false));
        oldPipeline.addFirst(new TestInboundHandler(oldCount, true));
        oldPipeline.addFirst(new TestOutboundHandler());
        oldPipeline.addFirst(new TestInboundHandler(oldCount, true));
        oldPipeline.fireChannelRegistered();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int cowPipeline() {
        cowPipeline.fireChannelRead(READ_OBJECT);
        return cowCount.value;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int oldPipeline() {
        oldPipeline.fireChannelRead(READ_OBJECT);
        return oldCount.value;
    }

    private static final class TestInboundHandler extends ChannelInboundHandlerAdapter {
        private final MutableInteger mi;
        private final boolean forwardRead;

        TestInboundHandler(MutableInteger mi,
                           boolean forwardRead) {
            this.mi = mi;
            this.forwardRead = forwardRead;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ++mi.value;
            if (forwardRead) {
                ctx.fireChannelRead(msg);
            }
        }
    }

    private static final class TestOutboundHandler extends ChannelOutboundHandlerAdapter {
    }

    private static final class MutableInteger {
        int value;
    }
}
