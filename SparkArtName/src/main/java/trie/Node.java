package trie;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Node implements Serializable {

    private TreeMap<Integer, ArrayList<Node>> children = new TreeMap<>();
    private int level = 0;
    private int roadSegment;
    private Long2ObjectAVLTreeMap<IntArraySet> trajectoryStartTime = new Long2ObjectAVLTreeMap<>();

    public void setLevel(int level) {
        this.level = level;
    }

    public int getRoadSegment() {

        return roadSegment;
    }

    private Node(int segment) {
        roadSegment = segment;
    }

    public static Node getNode(int newWord) {

        return new Node(newWord);
    }


    public ArrayList<Node> getChildren(int roadSegment) {
        ArrayList<Node> children = this.children.get(roadSegment);
        return children;
    }

    public Node addChild(int newWord) {
        Node n = getNode(newWord);
        n.setLevel(level + 1);
        ArrayList<Node> nodeChildren = children.get(newWord);

        if (nodeChildren == null) {
            nodeChildren = new ArrayList<>();
            children.put(newWord, nodeChildren);
        }

        nodeChildren.add(n);


        return n;
    }


    public Collection<Integer> getTrajectories(long startingTime, long endingTime) {
        SortedMap<Long, IntArraySet> startingEntries = trajectoryStartTime.subMap(startingTime, endingTime);


        final TreeSet<Integer> startingAnswer = new TreeSet<>();
        for (Map.Entry<Long, IntArraySet> entry : startingEntries.entrySet()) {
            startingAnswer.addAll(entry.getValue());
        }

        return startingAnswer;
    }


    public void addStartingTime(long timestamp, int trajectoryID) {
        if (trajectoryStartTime.containsKey(timestamp)) {
            trajectoryStartTime.get(timestamp).add(trajectoryID);
        } else {
            IntArraySet trajSet = new IntArraySet();
            trajSet.add(trajectoryID);
            trajectoryStartTime.put(timestamp, trajSet);
        }
    }


    public List<Node> getAllChildren() {
        return children.values().stream().flatMap(l -> l.stream()).collect(Collectors.toList());
    }

    public void addToRoot(int roadSegment, Node child) {

        ArrayList<Node> rootChildren = children.get(roadSegment);

        if (rootChildren == null) {
            rootChildren = new ArrayList<>();
            children.put(roadSegment, rootChildren);
        }

        rootChildren.add(child);

    }
}
