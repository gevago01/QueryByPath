package trie;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2IntAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import utilities.Connection;
import utilities.NodeRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class Node implements Serializable {

    private TreeMap<Integer, Connection> children = new TreeMap<>();

    private Long2IntAVLTreeMap timeToTID = new Long2IntAVLTreeMap();
    private int level = 0;
    private int word;
    public void setLevel(int level) {
        this.level = level;
    }

    public int getWord() {

        return word;
    }

    public void setWord(int word) {
        this.word = word;
    }


    private Node(int newWord) {
        word = newWord;
    }

    public static Node getNode(int newWord) {

        return new Node(newWord);
    }


    public Node getChildren(int roadSegment) {
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

    public Node addChild(int newWord) {
//        Node n = getNode(newWord.intern());
        Node n = getNode(newWord);
        n.setLevel(level + 1);
        Connection c = new Connection(n);
        children.put(n.getWord(), c);
//        children.put(n.getWord(), n);

//        children.add(n);

        return n;
    }

    public void addTrajectory(long timestamp, int trajectoryID) {
        timeToTID.put(timestamp, trajectoryID);

    }

    //    public Set<Long> getTrajectories(long startingTime, long endingTime) {
    public Collection<Integer> getTrajectories(long startingTime, long endingTime) {
        SortedMap<Long, Integer> entries = timeToTID.subMap(startingTime, endingTime);

        return entries.values();
    }

    public Integer getLevel() {
        return level;
    }

}
