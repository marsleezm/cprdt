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
 * Basic interface for representing an update operation on a CRDT. Immutable.
 * 
 * @author nmp, annettebieniusa, mzawirsk
 */
public interface CRDTUpdate<V extends CRDT<V>> {
    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);
    
    /**
     * 
     * @return Set of particles of the CRDT needed to apply this update
     *  or null if a full CRDT is needed
     */
    /* Not supported
     * Is there a useful case where this is not empty and different from affectedParticles() ?
    Set<Object> requiredParticles();
    */
    
    /**
     * 
     * @return Set of particles of the CRDT this update can affect (might change its state)
     *  or null if the complete CRDT is affected
     */
    Set<Object> affectedParticles();

    /**
     * @return estimated size of pure "value" in the update, as opposed to
     *         metadata; for PERFORMANCE MEASUREMENTS purposes only, return null
     *         if in doubt
     */
    Object getValueWithoutMetadata();
}
