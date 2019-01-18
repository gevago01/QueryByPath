package trie;

import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import utilities.Connection;
import utilities.NodeRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class Node implements Serializable {

    private TreeMap<Long, Connection> children = new TreeMap<>();
//    private TreeMap<Long, Node> children = new TreeMap<>();

    private Long2LongAVLTreeMap timeToTID = new Long2LongAVLTreeMap();
    private int level = 0;
    private long word;
    public void setLevel(int level) {
        this.level = level;
    }
    private int asd;


//    public Collection<Node> getNodes() {
//        Set<Node> setOfNodes = new TreeSet<>();
//        Collection<Connection> allConnections = children.values();
//
//        for (Connection c : allConnections) {
//            setOfNodes.add(c.getDestination());
//        }
//        return setOfNodes;
//    }

    public long getWord() {

        return word;
    }

    public void setWord(long word) {
        this.word = word;
    }


    private Node(long newWord) {
        word = newWord;
    }

    public static Node getNode(long newWord) {

        return new Node(newWord);
    }

//    public Connection getConnection(String dummyConnection){
//            for (Connection c:children){
//                if (c.getDestination().getWord().equals(dummyConnection)){
//                    return c;
//                }
//            }
//        return null;
//    }

    public Node getChildren(Long roadSegment) {
        //list implementation
//        for (Node n:children) {
//            if (n.getWord()==roadSegment){
//                return n;
//            }
//        }
//        return null;


//        return children.get(roadSegment);
        //connection implementation
        Connection connection = children.get(roadSegment);//getConnection(roadSegment);
        return connection == null ? null : connection.getDestination();
    }

    public Node addChild(long newWord) {
//        Node n = getNode(newWord.intern());
        Node n = getNode(newWord);
        n.setLevel(level + 1);
        Connection c = new Connection(n);
        children.put(n.getWord(), c);
//        children.put(n.getWord(), n);

//        children.add(n);

        return n;
    }

    public void addTrajectory(long timestamp, long trajectoryID) {
        timeToTID.put(timestamp, trajectoryID);

    }

    //    public Set<Long> getTrajectories(long startingTime, long endingTime) {
    public Collection<Long> getTrajectories(long startingTime, long endingTime) {
        SortedMap<Long, Long> entries = timeToTID.subMap(startingTime, endingTime);

        return entries.values();
    }

    public Integer getLevel() {
        return level;
    }

}
