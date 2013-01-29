/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.net.impl;

public interface NetworkingConstants {

    static final int TCP_CONNECTION_TIMEOUT = 10000;

    static final int NETTY_CORE_THREADS = 256;
    static final int NETTY_WRITEBUFFER_DEFAULTSIZE = 2048;
    static final int NETTY_MAX_TOTAL_MEMORY = 128 * (1 << 20);
    static final int NETTY_MAX_MEMORY_PER_CHANNEL = 1 * (1 << 20);

    static final int KRYOBUFFER_INITIAL_CAPACITY = 2048;

    static final int KRYOBUFFERPOOL_CLT_MAXSIZE = 8;
    static final int KRYOBUFFERPOOL_SRV_MAXSIZE = 1024;
    static final int KRYOBUFFERPOOL_DELAY = 100;

    static final int RPC_DEFAULT_TIMEOUT = 60000;
    static final long RPC_MAX_SERVICE_ID = 1L << 16;
    static final long RPC_MAX_SERVICE_ID_MASK = (1L << 16) - 1L;

    static final int RPC_GC_STALE_HANDLERS_PERIOD = 15 * 60;

    static enum NIO_ReadBufferPoolPolicy {
        POLLING, BLOCKING
    };

    static enum NIO_WriteBufferPoolPolicy {
        POLLING, BLOCKING
    };

    static enum NIO_ReadBufferDispatchPolicy {
        READER_EXECUTES, USE_THREAD_POOL
    }

    static final int NIO_EXEC_QUEUE_SIZE = 16;
    static final int NIO_CORE_POOL_THREADS = 5;
    static final int NIO_MAX_POOL_THREADS = 12;

    static final int NIO_MAX_IDLE_THREAD_IMEOUT = 30;

    static final int RPC_CONNECTION_RETRIES = 3;
    static final int RPC_CONNECTION_RETRY_DELAY = 250;

    static final int DHT_CLIENT_RETRIES = 3;
    static final int DHT_CLIENT_TIMEOUT = 100;

}
