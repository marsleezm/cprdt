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
package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_Request implements RpcMessage {

    public DHT.Key key;
    public boolean redirected;
    public DHT.Message payload;
    public boolean expectingReply;

    DHT_Request() {
    }

    public DHT_Request(DHT.Key key, DHT.Message payload) {
        this(key, payload, false);
    }

    public DHT_Request(DHT.Key key, DHT.Message payload, boolean expectingReply) {
        this.key = key;
        this.payload = payload;
        this.redirected = false;
        this.expectingReply = expectingReply;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((DHT_StubHandler) handler).onReceive(conn, this);
    }

    public String toString() {
        return super.toString();
    }

}
