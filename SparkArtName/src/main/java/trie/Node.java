package trie;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import utilities.Connection;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Node implements Serializable {

    private TreeMap<Integer, Connection> children = new TreeMap<>();
    private int level = 0;
    private int roadSegment;
    private Long2ObjectAVLTreeMap<IntArraySet> timeToTID =new Long2ObjectAVLTreeMap<>();
    private Long2ObjectAVLTreeMap<IntArraySet> trajectoryStartTime =new Long2ObjectAVLTreeMap<>();
    private Long2ObjectAVLTreeMap<IntArraySet> trajectoryEndTime =new Long2ObjectAVLTreeMap<>();

    public boolean checkIfRSExists(int k){
        return children.keySet().contains(k);
    }

    //    private Long2IntAVLTreeMap timeToTID = new Long2IntAVLTreeMap();
//    private Long2IntAVLTreeMap timeToTID = new Long2IntAVLTreeMap();
    //    private TreeMap<Integer, Set<Integer>>



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


    public Node getChildren(int roadSegment) {
        //list implementation
//        for (Node n:children) {
//            if (n.getRoadSegment()==roadSegment){
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
        children.put(n.getRoadSegment(), c);
//        children.put(n.getRoadSegment(), n);

//        children.add(n);

        return n;
    }


    public Collection<Integer> getTrajectories(long startingTime, long endingTime) {
        SortedMap<Long, IntArraySet> startingEntries = trajectoryStartTime.subMap(startingTime, endingTime);
        SortedMap<Long, IntArraySet> endingEntries = trajectoryEndTime.subMap(startingTime, endingTime);



        final TreeSet<Integer> startingAnswer=new TreeSet<>();
        for (Map.Entry<Long, IntArraySet> entry:startingEntries.entrySet()) {
            startingAnswer.addAll(entry.getValue());
        }

        final TreeSet<Integer> endingAnswer=new TreeSet<>();
        for (Map.Entry<Long, IntArraySet> entry:endingEntries.entrySet()) {
            endingAnswer.addAll(entry.getValue());
        }

        startingAnswer.retainAll(endingAnswer);
        //startingAnswer contains elements in both sets
        return startingAnswer;
    }

    public void addTrajectory(long timestamp, int trajectoryID) {
        if (timeToTID.containsKey(timestamp)){
            timeToTID.get(timestamp).add(trajectoryID);
        }
        else{
            IntArraySet trajSet=new IntArraySet();
            trajSet.add(trajectoryID);
            timeToTID.put(timestamp,trajSet);
        }
    }

    public void addStartingTime(long timestamp, int trajectoryID) {
        if (trajectoryStartTime.containsKey(timestamp)){
            trajectoryStartTime.get(timestamp).add(trajectoryID);
        }
        else{
            IntArraySet trajSet=new IntArraySet();
            trajSet.add(trajectoryID);
            trajectoryStartTime.put(timestamp,trajSet);
        }
    }

    public void addEndingTime(long timestamp, int trajectoryID) {
        if (trajectoryEndTime.containsKey(timestamp)){
            trajectoryEndTime.get(timestamp).add(trajectoryID);
        }
        else{
            IntArraySet trajSet=new IntArraySet();
            trajSet.add(trajectoryID);
            trajectoryEndTime.put(timestamp,trajSet);
        }
    }
//    private TreeMap<Long, IntArraySet> timeToTID = new TreeMap<>();
//        public Collection<Integer> getTrajectories(long startingTime, long endingTime) {
//        SortedMap<Long, IntArraySet> entries = timeToTID.subMap(startingTime, endingTime);
//        final TreeSet<Integer> answer=new TreeSet<>();
//        for (Map.Entry<Long, IntArraySet> entry:entries.entrySet()) {
//            answer.addAll(entry.getValue());
//        }
//        return answer;
//}
//
//    public void addTrajectory(long timestamp, int trajectoryID) {
//        if (timeToTID.containsKey(timestamp)){
//            timeToTID.get(timestamp).add(trajectoryID);
//        }
//        else{
//            IntArraySet trajSet=new IntArraySet();
//            trajSet.add(trajectoryID);
//            timeToTID.put(timestamp,trajSet);
//        }
//    }

//    private ArrayList<TimeTrajIDTuple> timeToTID = new ArrayList<>();
//    public void addTrajectory(long timestamp, int trajectoryID) {
//
//        Optional<TimeTrajIDTuple> found = timeToTID.stream().filter(tuple -> tuple.getTimestamp() == timestamp).findFirst();
//        TimeTrajIDTuple timeTrajIDTuple = null;
//        try {
//            timeTrajIDTuple = found.get();
//        } catch (NoSuchElementException nsee) {
//            timeTrajIDTuple = new TimeTrajIDTuple();
//            timeTrajIDTuple.setTimestamp(timestamp);
//            timeToTID.add(timeTrajIDTuple);
//        }
//        timeTrajIDTuple.addTrajID(trajectoryID);
//
//
//    }
//
//    public Collection<Integer> getTrajectories(long startingTime, long endingTime) {
//        return timeToTID.stream().filter(t -> t.getTimestamp() >= startingTime && t.getTimestamp() <= endingTime).flatMap(t -> t.getTrajIDs().stream()).collect(toList());
//    }

    public Integer getLevel() {
        return level;
    }

    public List<Node> getAllChildren() {
        return children.values().stream().map(Connection::getDestination).collect(Collectors.toList());
    }
    public List<Connection> getAllChildren2() {
        return children.values().stream().collect(Collectors.toList());
    }

    public Collection<Connection> getAllChildrenConns() {
        return children.values();
    }
}
