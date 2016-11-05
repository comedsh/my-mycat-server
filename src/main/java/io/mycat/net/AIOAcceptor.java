/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.net.factory.FrontendConnectionFactory;

/**
 * @author mycat
 */
public final class AIOAcceptor implements SocketAcceptor,
		CompletionHandler<AsynchronousSocketChannel, Long> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AIOAcceptor.class);
	private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

	private final int port;
	private final AsynchronousServerSocketChannel serverChannel;
	private final FrontendConnectionFactory factory;

	private long acceptCount;
	private final String name;

	public AIOAcceptor(String name, String ip, int port,
			FrontendConnectionFactory factory, AsynchronousChannelGroup group)
			throws IOException {
		this.name = name;
		this.port = port;
		this.factory = factory;
		serverChannel = AsynchronousServerSocketChannel.open(group);
		/** 设置TCP属性 */
		serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
		// backlog=100
		serverChannel.bind(new InetSocketAddress(ip, port), 100);
	}

	public String getName() {
		return name;
	}
	
	/**
	 * 入口方法，在 {@link MycatServer#startup()} 方法中调用。
	 */
	public void start() {
		this.pendingAccept();
	}

	public int getPort() {
		return port;
	}

	public long getAcceptCount() {
		return acceptCount;
	}
	
	/**
	 * 
	 * @param channel
	 * @param id -> attachment
	 */
	private void accept(NetworkChannel channel, Long id) {
		try {
			FrontendConnection c = factory.make(channel);
			c.setAccepted(true);
			c.setId(id); // 作为 FrontendConnnection 的 ID
			NIOProcessor processor = MycatServer.getInstance().nextProcessor();
			c.setProcessor(processor);
			c.register(); // -> AIOSocketWR#asynRead
		} catch (Exception e) {
		    LOGGER.error("AioAcceptorError", e);
			closeChannel(channel);
		}
	}

	private void pendingAccept() {
		if (serverChannel.isOpen()) {
			/** 这里的代码就太关键了，serverChannel 开始监听客户端的连接了，并且将 AIOAcceptor 作为 CompletionHandler 回调对象进行监听 **/
			/** 当有数据返回以后，就直接调用 AIOAcceptor#completed(AsynchronousSocketChannel result, Long id) **/
			/** 注意，ID_GENERATOR.getId() 是作为 attachment 进行 accept **/
			serverChannel.accept(ID_GENERATOR.getId(), this);
		} else {
			throw new IllegalStateException(
					"MyCAT Server Channel has been closed");
		}

	}

	/**
	 * @param channel -> Client 与 Mycat 连接的 channel
	 * @param id -> attachment
	 */
	@Override
	public void completed(AsynchronousSocketChannel channel, Long id) {
		accept(channel, id);
		// next pending waiting
		pendingAccept();

	}

	@Override
	public void failed(Throwable exc, Long id) {
		LOGGER.info("acception connect failed:" + exc);
		// next pending waiting
		pendingAccept();

	}

	private static void closeChannel(NetworkChannel channel) {
		if (channel == null) {
			return;
		}
		try {
			channel.close();
		} catch (IOException e) {
	        LOGGER.error("AioAcceptorError", e);
		}
	}

	/**
	 * 前端连接ID生成器
	 * 
	 * @author mycat
	 */
	private static class AcceptIdGenerator {

		private static final long MAX_VALUE = 0xffffffffL;

		private AtomicLong acceptId = new AtomicLong();
		private final Object lock = new Object();

		private long getId() {
			long newValue = acceptId.getAndIncrement();
			if (newValue >= MAX_VALUE) {
				synchronized (lock) {
					newValue = acceptId.getAndIncrement();
					if (newValue >= MAX_VALUE) {
						acceptId.set(0);
					}
				}
				return acceptId.getAndDecrement();
			} else {
				return newValue;
			}
		}
	}
}
