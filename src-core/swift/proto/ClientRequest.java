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
package swift.proto;

import sys.net.api.rpc.RpcMessage;

/**
 * Abstract client to server request, identifying a client by its unique id.
 * 
 * @author mzawirski
 */
public abstract class ClientRequest implements RpcMessage {
    protected String clientId;
    // TODO: ideally, set it only in session init. request
    protected boolean disasterSafeSession;

    // Fake constructor for Kryo serialization. Do NOT use.
    public ClientRequest() {
    }

    public ClientRequest(final String clientId, boolean disasterSafeSession) {
        this.clientId = clientId;
        this.disasterSafeSession = disasterSafeSession;
    }

    /**
     * @return unique client id of originator of this request
     */
    public String getClientId() {
        return clientId;
    }

    public boolean isDisasterSafeSession() {
        return disasterSafeSession;
    }
}
