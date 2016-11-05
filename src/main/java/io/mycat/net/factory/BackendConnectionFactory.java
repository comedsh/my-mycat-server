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
package io.mycat.net.factory;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

import io.mycat.MycatServer;

/**
 * @author mycat
 */
public abstract class BackendConnectionFactory {

	/**
	 * 这里需要特别注意的是，
	 * 
	 * NIO：作者并没有真真的开启与 database 之间的连接，只是简单的初始化了一个 SocketChannel <br>
	 * 作者这样做的意图是，把 SocketChannel 绑定到当前的 AbstractConnection <br>
	 * 
	 * 同时需要注意的是，这里作者的解耦做得相当不好，同时处理了 NIO 和 AIO 的情形，如果是我，我会分开 <br>
	 * 
	 * @param isAIO
	 * @return
	 * @throws IOException
	 */
	protected NetworkChannel openSocketChannel(boolean isAIO)
			throws IOException {
		if (isAIO) {
			return AsynchronousSocketChannel
                .open(MycatServer.getInstance().getNextAsyncChannelGroup());
		} else {
			SocketChannel channel = null;
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			return channel;
		}

	}

}