package utilities;

import trie.Node;

import java.io.Serializable;

/**
 * Created by giannis on 22/12/18.
 */
public class NodeRef implements Serializable{

//    private transient Node node;
    private  Node node;

    public int getUid() {
        return uid;
    }

    private final int uid;
//    public static TreeMap<Long, Node> nodePool = new TreeMap<>();

    public NodeRef(Node node) {
        this.node = node;
        this.uid = node.getRoadSegment();
//        Node nodeReference = nodePool.get(node.getRoadSegment());
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
        return Integer.hashCode(uid);
    }

}
