package com.adsk.miniframework.webapp.olds;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpInitialiser extends ChannelInitializer<SocketChannel>
{
	public HttpInitialiser(){}
	
	@Override
	public void initChannel(SocketChannel channel)
	{
		ChannelPipeline pipeline = channel.pipeline();
		pipeline.addLast(new HttpRequestDecoder());
		pipeline.addLast(new HttpResponseEncoder());
		pipeline.addLast(new HttpHandler());
	}
}
