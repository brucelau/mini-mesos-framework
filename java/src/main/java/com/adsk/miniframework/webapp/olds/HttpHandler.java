package com.adsk.miniframework.webapp.olds;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;

public class HttpHandler extends SimpleChannelInboundHandler<Object>
{
	private HttpRequest request;

	/** Buffer that stores the response content */
	private final StringBuilder buffer = new StringBuilder();

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx)
	{
		ctx.flush();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg)
	{
		if (msg instanceof HttpRequest)
		{
			HttpRequest request = this.request = (HttpRequest) msg;
		}
		
	}

}
