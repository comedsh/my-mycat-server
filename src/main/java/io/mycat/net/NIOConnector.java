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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import java.util.concurrent.atomic.AtomicLong;
/**
 * @author mycat
 */
public final class NIOConnector extends Thread implements SocketConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
	public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

	private final String name;
	private final Selector selector;
	private final BlockingQueue<AbstractConnection> connectQueue;
	private long connectCount;
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool)
			throws IOException {
		super.setName(name);
		this.name = name;
		this.selector = Selector.open();
		this.reactorPool = reactorPool;
		this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

	public void postConnect(AbstractConnection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	/**
	 * 
	 * 今天读源码读到这里似乎开窍了，Thread Pool 中的线程用不用每次都初始化一个新的线程，其实，完全是受自己控制的... 
	 * 之前读源码，只要 add 一个新的 Runnable 到线程池中，一定会 new 一个新的 Thread，因为 Runnable 必须依赖于 Thread 才能执行.. 
	 * 
	 * 如今，这里的 Thread pool 中的线程是通过 selector 来进行休眠，当有数据来的时候，在唤醒，目的是不让这个线程死掉，也就是说，不用重新创建一个新的线程来处理新的数据..
	 * 这样一来，我的线程池中的线程数量将会始终维持一定..
	 * 
	 * ok，上述的描述，在描绘 NIOConnector 本身不太合适，因为 NIOConnector 本身并没有使用 ThreadPool，但是用在 NIOReactor 上就在合适不过了...  
	 * 因为 NIOReactor 使用的就是 ThreadPool
	 * 
	 */
	@Override
	public void run() {
		final Selector tSelector = this.selector;
		for (;;) {
			++connectCount;
			try {
			    tSelector.select(1000L);
				connect(tSelector);
				Set<SelectionKey> keys = tSelector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						Object att = key.attachment();
						if (att != null && key.isValid() && key.isConnectable()) {
							finishConnect(key, att);
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			} catch (Exception e) {
				LOGGER.warn(name, e);
			}
		}
	}

	private void connect(Selector selector) {
		AbstractConnection c = null;
		while ((c = connectQueue.poll()) != null) {
			try {
				SocketChannel channel = (SocketChannel) c.getChannel();
				channel.register(selector, SelectionKey.OP_CONNECT, c);
				channel.connect(new InetSocketAddress(c.host, c.port));
				
			} catch (Exception e) {
				c.close(e.toString());
			}
		}
	}

	private void finishConnect(SelectionKey key, Object att) {
		BackendAIOConnection c = (BackendAIOConnection) att;
		try {
			if (finishConnect(c, (SocketChannel) c.channel)) {
				clearSelectionKey(key); /** 作者这里为什么可以 clear 掉这个 key，是因为该 key 返回的是连接成功消息，一旦连接成功，自然也就不需要了... **/
				c.setId(ID_GENERATOR.getId());
				NIOProcessor processor = MycatServer.getInstance()
						.nextProcessor();
				c.setProcessor(processor);
				NIOReactor reactor = reactorPool.getNextReactor();
				reactor.postRegister(c);
				c.onConnectfinish();
			}
		} catch (Exception e) {
			clearSelectionKey(key);
            c.close(e.toString());
			c.onConnectFailed(e);

		}
	}

	private boolean finishConnect(AbstractConnection c, SocketChannel channel)
			throws IOException {
		if (channel.isConnectionPending()) {
			channel.finishConnect();

			c.setLocalPort(channel.socket().getLocalPort());
			return true;
		} else {
			return false;
		}
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}

	/**
	 * 后端连接ID生成器
	 * 
	 * @author mycat
	 */
	public static class ConnectIdGenerator {

		private static final long MAX_VALUE = Long.MAX_VALUE;
		private AtomicLong connectId = new AtomicLong(0);

		public long getId() {
			return connectId.incrementAndGet();
		}
	}

}
