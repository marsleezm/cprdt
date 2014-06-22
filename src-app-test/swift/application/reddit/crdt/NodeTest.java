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
package swift.application.reddit.crdt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.application.reddit.crdt.DecoratedNode;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.SwiftException;

public class NodeTest {
    
    Node<Integer> root;
    
    @Before
    public void setUp() throws SwiftException {
        root = new Node<Integer>(null, null);
    }
    
    @Test
    public void equalityTest() {
        assertTrue(root.equals(root));
        
        Node<Integer> a = new Node<Integer>(root, 1);
        Node<Integer> aprime = new Node<Integer>(root, 1);
        Node<Integer> b = new Node<Integer>(a, 1);
        
        assertTrue(a.equals(aprime));
        
        assertTrue(!a.equals(b));
        assertTrue(!b.equals(a));
    }
}
