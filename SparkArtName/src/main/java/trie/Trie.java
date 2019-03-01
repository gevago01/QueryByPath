package trie;


import utilities.Trajectory;

import java.io.Serializable;
import java.util.*;

public class Trie implements Serializable {
    private Node root = Node.getNode(Long.MAX_VALUE);
    private long startingRoadSegment = Long.MAX_VALUE;
    public int timeSlice;
    private int horizontalTrieID;
    private int verticalID;

    public int getTrajectoryCounter() {
        return trajectoryCounter;
    }

    private int trajectoryCounter = 0;




    public void setStartingRoadSegment(long startingRoadSegment) {
        this.startingRoadSegment = startingRoadSegment;
    }

    public Integer getTimeSlice() {
        return timeSlice;
    }

    public void setTimeSlice(Integer timeSlice) {
        this.timeSlice = timeSlice;
    }

    public int getHorizontalTrieID() {
        return horizontalTrieID;
    }


    public Node getRoot() {
        return root;
    }


    public void insertTrajectory2(List<Long> roadSegments, long trajectoryID, long startingTime, long endingTime) {
        Node currentNode, child = root;
        Long previousRoadSegment = -1l;


        if (startingRoadSegment == Long.MAX_VALUE) {
            //this is only used for vertical partitioning
            startingRoadSegment = roadSegments.get(0);//.intern();
        }

        getRoot().addTrajectory(startingTime, trajectoryID);
        for (int i = 0; i < roadSegments.size(); i++) {
            long roadSegment = roadSegments.get(i);//.intern();
            if (roadSegment == previousRoadSegment) {
                continue;
            }
            currentNode = child;

            child = currentNode.getChildren(roadSegment);

            if (child == null) {
                child = currentNode.addChild(roadSegment);
            }

            previousRoadSegment = roadSegment;
        }
        child.addTrajectory(endingTime, trajectoryID);


    }
//  less memory efficient of insert, inserts all timestamps to the index
//    public void insertTrajectory(List<String> roadSegments, long trajectoryID, List<Long> timestamps) {
//        Node currentNode , child = root;
//        String previousRoadSegment = null;
//
//        if (roadSegments.isEmpty() || timestamps.isEmpty()){
//            System.err.println("lists are empty");;
//            System.exit(-1);
//        }
//
//        assert (timestamps.size() == roadSegments.size());
//
//        if (startingRoadSegment.isEmpty()) {
//            //this is only used for vertical partitioning
//            startingRoadSegment = roadSegments.get(0);
//        }
//
//        if (roadSegments.size()>max){
//            max=roadSegments.size();
//        }
//        for (int i = 0; i < roadSegments.size(); i++) {
//            String roadSegment = roadSegments.get(i).intern();
//            if (roadSegment.equals(previousRoadSegment)) {
//                assert (child != null);
//                child.addTrajectory(timestamps.get(i), trajectoryID);
//                ++max;
//                continue;
//            }
//            currentNode = child;
//
//            Map<String, Node> nodeChildren = currentNode.getChildren();
//            child = nodeChildren.get(roadSegment);
//
//            if (child == null) {
//                child = currentNode.addChild(roadSegment);
//            }
//
//            child.addTrajectory(timestamps.get(i), trajectoryID);
//
//            previousRoadSegment = roadSegment;
//            ++max;
//
//        }
//
//    }



    /**
     * This is only valid for vertical partitioning
     *
     * @return
     */
    public long getStartingRS() {
        if (startingRoadSegment == Long.MAX_VALUE) {
            System.err.println("Wrong Usage. Insert trajectories to the trie first.");
            System.exit(-2);
        }
        return startingRoadSegment;
    }


    /**
     * This method does not answer strict path queries
     * If query is ABCD, it returns trajectories that have only passed through AB for example
     *
     * @param q
     * @return
     */
    public Set<Long> queryIndex(Query q) {


        Node currentNode = root;
        Set<Long> answer = new TreeSet<>();

        for (int i = 0; i < q.getPathSegments().size(); i++) {

            long roadSegment = q.getPathSegments().get(i);


            if (currentNode.getWord()==(roadSegment)) {
                continue;
            }
            Node child = currentNode.getChildren(roadSegment);

            if (child == null) {
                //no matching result
//                System.err.println("no matching result");
//                System.exit(1);
                break;
            }
            else{
                //here filter time
                answer.addAll(child.getTrajectories(q.getStartingTime(), (q.getEndingTime() + 1)));
                currentNode = child;
            }
        }

        return answer;
    }



    public void setHorizontalTrieID(int horizontalTrieID) {
        this.horizontalTrieID = horizontalTrieID;
    }

    @Override
    public String toString() {
        return "TriePrint{" +
                "root=" + root.getWord() +
                ", startingRoadSegment='" + startingRoadSegment + '\'' +
                ", timeSlice=" + timeSlice +
                ", horizontalTrieID=" + horizontalTrieID +
                '}';
    }




    public void insertTrajectory2(Trajectory traj) {
        ++trajectoryCounter;
        insertTrajectory2(traj.roadSegments, traj.trajectoryID, traj.getStartingTime(), traj.getEndingTime());
    }

    public void setVerticalID(int verticalID) {
        this.verticalID = verticalID;
    }

    public int getVerticalID() {
        return verticalID;
    }
}
