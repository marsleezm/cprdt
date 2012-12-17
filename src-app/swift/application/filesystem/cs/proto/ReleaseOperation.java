/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 University of Kaiserslautern
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
package swift.application.filesystem.cs.proto;

import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import fuse.FuseException;

public class ReleaseOperation extends FuseRemoteOperation {

    private String path;
    private Object fileHandle;
    private int flags;

    ReleaseOperation() {
    }

    public ReleaseOperation(String path, Object fileHandle, int flags) {
        this.path = path;
        this.fileHandle = fileHandle;
        this.flags = flags;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler) handler).release(path, SwiftFuseServer.c2s_fh(fileHandle), flags);
            SwiftFuseServer.disposeFh(fileHandle);
            handle.reply(new FuseOperationResult(res));
        } catch (FuseException e) {
            handle.reply(new FuseOperationResult());
        }
    }

}
