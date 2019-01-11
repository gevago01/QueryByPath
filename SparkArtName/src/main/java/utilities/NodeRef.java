package utilities;

import trie.Node;

import java.io.Serializable;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by giannis on 22/12/18.
 */
public class NodeRef implements Serializable{

//    private transient Node node;
    private  Node node;

    public long getUid() {
        return uid;
    }

    private final long uid;
//    public static TreeMap<Long, Node> nodePool = new TreeMap<>();

    public NodeRef(Node node) {
        this.node = node;
        this.uid = node.getWord();
//        Node nodeReference = nodePool.get(node.getWord());
//        if (nodeReference == null) {
//            nodePool.put(uid, node);
//        }
    }

    public Node resolve() {
        return node;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof NodeRef) && uid == (((NodeRef) o).getUid());
    }

    @Override
    public int hashCode() {
        return Long.hashCode(uid);
    }

}
