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
package swift.crdt.core;

import java.util.Set;

/**
 * See CRDTUpdate, abstract class to have a default implementation 
 * not supporting partial replicas
 */
public abstract class AbstractCRDTUpdate<V extends CRDT<V>> implements CRDTUpdate<V> {
    
    public abstract void applyTo(V crdt);
    
    @Override
    public Set<Object> requiredParticles() {
        return null;
    }
    
    @Override
    public Set<Object> affectedParticles() {
        return null;
    }

    @Override
    public Object getValueWithoutMetadata() {
        return null;
    }
}
