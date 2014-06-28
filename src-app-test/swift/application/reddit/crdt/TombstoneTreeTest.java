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

public class TombstoneTreeTest {
    TxnHandle txn;
    TombstoneTreeCRDT<Integer> i;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        i = txn.get(new CRDTIdentifier("A", "Int"), true, TombstoneTreeCRDT.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void emptyTest() {
        // Check node does not exist
        assertTrue(!i.has(new Node(i.getRoot(), 1)));
        assertTrue(!i.isRemoved(new Node(i.getRoot(), 1)));
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void insertTest() {
        int v = 5;
        int w = 6;
        int x = 7;
        int y = 8;
        int z = 9;
        
        // insert one element
        Node vNode = new Node(i.getRoot(), v);
        Node wNode = new Node(i.getRoot(), w);
        Node xNode = new Node(wNode, x);
        
        i.add(vNode);
        assertTrue(i.has(vNode));
        assertTrue(!i.isRemoved(vNode));
        assertTrue(!i.has(wNode));

        // insertion should be idempotent
        i.add(vNode);
        assertTrue(i.has(vNode));
        
        // Add child with same value as parent
        i.add(wNode);
        i.add(xNode);
        assertTrue(i.has(xNode));
        // Try with new instance
        assertTrue(i.has(new Node(i.getRoot(), v)));

        Node yNode = new Node(wNode, y);
        Node zNode = new Node(yNode, z);
        // Try adding child to non existing node
        assertTrue(!i.isAdded(yNode));
        i.add(zNode);
        assertTrue(!i.isAdded(zNode));
    }

    @Test
    public void deleteTest() {
        int v = 5;
        int w = 7;
        Node vNode = new Node(i.getRoot(), v);
        Node wNode = new Node(vNode, w);
        i.add(vNode);
        i.add(wNode);

        i.remove(vNode);
        assertTrue(i.isAdded(vNode));
        assertTrue(i.isRemoved(vNode));
        assertTrue(i.isAdded(wNode));
        assertTrue(!i.isRemoved(wNode));

        // remove should be idempotent
        i.remove(vNode);
        assertTrue(i.isAdded(vNode));
        assertTrue(i.isRemoved(vNode));
        assertTrue(i.isAdded(wNode));
        assertTrue(!i.isRemoved(wNode));

        i.remove(wNode);
        assertTrue(i.isAdded(vNode));
        assertTrue(i.isRemoved(vNode));
        assertTrue(i.isAdded(wNode));
        assertTrue(i.isRemoved(wNode));
    }
    
    @Test
    public void deleteThenAddTest() {
        int v = 5;
        int w = 7;
        Node vNode = new Node(i.getRoot(), v);
        Node wNode = new Node(vNode, w);
        
        i.add(vNode);
        i.add(wNode);

        i.remove(vNode);
        assertTrue(!i.has(vNode));
        assertTrue(i.isRemoved(vNode));
        assertTrue(i.has(wNode));

        // add after remove should be idempotent 
        i.add(vNode);
        assertTrue(!i.has(vNode));
        assertTrue(i.isRemoved(vNode));
        assertTrue(i.has(wNode));
    }
    
    @Test
    public void depthTest() {
        int v = 5;
        int w = 7;
        int x = 7;
        int y = 7;
        int z = 2;
        Node vNode = new Node(i.getRoot(), v);
        Node wNode = new Node(vNode, w);
        Node xNode = new Node(wNode, x);
        Node yNode = new Node(xNode, y);
        Node zNode = new Node(yNode, z);
        
        assertEquals(i.getRoot().depth(), 0);
        assertEquals(vNode.depth(), 1);
        assertEquals(wNode.depth(), 2);
        assertEquals(xNode.depth(), 3);
        assertEquals(yNode.depth(), 4);
        assertEquals(zNode.depth(), 5);
    }
    
    @Test
    public void childrenTest() {
        int v = 5;
        int w = 7;
        int x = 7;
        int y = 7;
        int z = 2;
        Node vNode = new Node(i.getRoot(), v);
        Node wNode = new Node(vNode, w);
        Node xNode = new Node(wNode, x);
        Node yNode = new Node(xNode, y);
        Node zNode = new Node(xNode, z);
        
        i.add(vNode);
        i.add(wNode);
        i.add(xNode);
        i.add(yNode);
        i.add(zNode);

        i.remove(zNode);
        
        Set<DecoratedNode<Node<Integer>,Integer>> children = i.decoratedChildrenOf(vNode);
        for (DecoratedNode n: children) {
            assertTrue(n.getNode().equals(wNode));
            assertTrue(!n.isTombstone());
        }
    }
}
