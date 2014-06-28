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
package swift.application.reddit.cprdt;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import swift.application.reddit.Date;
import swift.application.reddit.Link;
import swift.application.reddit.SortingOrder;
import swift.application.reddit.cprdt.IndexedVoteableSetCPRDT;
import swift.application.reddit.crdt.DecoratedNode;
import swift.application.reddit.crdt.Node;
import swift.application.reddit.crdt.VoteDirection;
import swift.crdt.TxnTester;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;

public class VoteableTreeTest {
    class Comment implements Date<Comment> {
        long date;
        
        public Comment(long date) {
            this.date = date;
        }
        
        @Override
        public int compareTo(Comment o) {
            if (this.date < o.date) {
                return -1;
            } else {
                if (this.date > o.date) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }

        @Override
        public long getDate() {
            return date;
        }
    }
    
    TxnHandle txn;
    VoteableTreeCPRDT<Comment, String> tree;
    
    // 39 comment nodes (3 levels, each node has 3 children)
    // In depth-first order
    List<SortedNode<Comment>> nodes;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = TxnTester.createIsolatedTxnTester();
        nodes = new ArrayList<SortedNode<Comment>>();
        tree = txn.get(new CRDTIdentifier("A", "Tree"), true, VoteableTreeCPRDT.class);
        
        // 28/04/2014
        long date = 1398725435;
        for (int i = 1; i <= 3; i++) {
            Comment comment = new Comment(date + (i * 10));
            SortedNode<Comment> node = new SortedNode<Comment>(tree.getRoot(), comment);
            nodes.add(node);
            for (int j = 1; j <= 3; j++) {
                comment = new Comment(date + (i * 10) + j * 10);
                node = new SortedNode<Comment>(node, comment);
                nodes.add(node);
                for (int k = 1; k <= 3; k++) {
                    comment = new Comment(date + (i * 10) + j * 10 + k * 10);
                    node = new SortedNode<Comment>(node, comment);
                    nodes.add(node);
                }
            }
        }
    }
    
    private void fillTree() throws VersionNotFoundException, NetworkException {
        for (SortedNode<Comment> node: nodes) {
            tree.add(node);
        }
    }

    @Test
    public void initTest() {
        assertTrue(tree.getValue().isEmpty());
    }

    @Test
    public void emptyTest() throws VersionNotFoundException, NetworkException {
        // Check node does not exist
        assertTrue(!tree.has(new SortedNode<Comment>(tree.getRoot(), new Comment(1))));
        assertTrue(!tree.isRemoved(new SortedNode<Comment>(tree.getRoot(), new Comment(1))));
        assertTrue(tree.getValue().isEmpty());
    }

    @Test
    public void dateIndexTest() throws VersionNotFoundException, NetworkException {
        int i = 0;
        fillTree();
        
        for (SortedNode<Comment> node: tree.applySortedSubtree(tree.getRoot(), Integer.MAX_VALUE, SortingOrder.OLD, Integer.MAX_VALUE)) {
            node.equals(nodes.get(i));
            i++;
        }
    }
    
    @Test
    public void topIndexTest() throws VersionNotFoundException, NetworkException {
        fillTree();
    }
    
    @Test
    public void copyTest() throws VersionNotFoundException, NetworkException {
        fillTree();
        VoteableTreeCPRDT<Comment, String> treeCopy = tree.copy();
        for (SortedNode<Comment> node: nodes) {
            assertTrue(treeCopy.has(node));
        }
        treeCopy = tree.copyFraction(Collections.singleton(nodes.get(0)));
        assertTrue(treeCopy.has(nodes.get(0)));
    }
    
    @Test
    public void constructorTest() {
        VoteableTreeCPRDT<Comment, String> tree = new VoteableTreeCPRDT<Comment, String>(new CRDTIdentifier("A", "Tree"));
    }
}
