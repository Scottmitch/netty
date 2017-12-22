package io.netty.channel;

public class CopyOnWriteChannelPipelineTest extends ChannelPipelineTest {
    @Override
    protected ChannelPipeline newPipeline(Channel channel) {
        return new CopyOnWriteChannelPipeline(channel);
    }
}
