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

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public class DefaultMessageHandler implements MessageHandler {

    final boolean silent;

    public DefaultMessageHandler() {
        this(false);
    }

    public DefaultMessageHandler(boolean silent) {
        this.silent = silent;
    }

    @Override
    public void onAccept(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onConnect(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onFailure(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onFailure(Endpoint dst, Message m) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onReceive(TransportConnection conn, Message m) {
        if (!silent)
            Thread.dumpStack();
    }

    @Override
    public void onClose(TransportConnection conn) {
        if (!silent)
            Thread.dumpStack();
    }
}
