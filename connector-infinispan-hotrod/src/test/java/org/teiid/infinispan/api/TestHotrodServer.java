/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.infinispan.api;

import org.infinispan.commons.api.BasicCache;
import org.junit.Before;
import org.teiid.infinispan.api.InfinispanConnection;
import static org.junit.Assert.*;

public class TestHotrodServer {
    HotRodTestServer server;

    @Before
    public void setup() {
        this.server = new HotRodTestServer();
    }

//    @After
//    public void tearDown() {
//        this.server.stop();
//    }

    //@Test
    public void testServer() throws Exception {
        InfinispanConnection connection = this.server.getConnection();
        BasicCache<Object, Object> cache = connection.getCache();

        cache.put(100, "hello");
        cache.put(101, "Infinispan");

        assertEquals("hello", cache.get(100));
        assertEquals("infinispan", cache.get(101));
    }

}