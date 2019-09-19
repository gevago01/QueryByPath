package trie;


import utilities.Trajectory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Trie implements Serializable {
    private Node root = Node.getNode(Integer.MAX_VALUE);

    public long getMinTrajLength() {
        return minTrajLength;
    }

    public long getMaxTrajLength() {
        return maxTrajLength;
    }

    private long minTrajLength=Long.MAX_VALUE;
    private long maxTrajLength=Long.MIN_VALUE;

    private long minStartingTime=Long.MAX_VALUE;
    private long maxStartingTime=Long.MIN_VALUE;
    private int minStartingRS=Integer.MAX_VALUE;

    public int getMinStartingRS() {
        return minStartingRS;
    }

    public int getMaxStartingRS() {
        return maxStartingRS;
    }

    private int maxStartingRS=Integer.MIN_VALUE;

    public long getHilbertValue() {
        return hilbertValue;
    }

    private long hilbertValue;

    public long getMinStartingTime() {
        return minStartingTime;
    }

    public long getMaxStartingTime() {
        return maxStartingTime;
    }









    public int getPartitionID() {
        return partitionID;
    }

    public int partitionID;
    private int horizontalTrieID;

    public int getTrajectoryCounter() {
        return trajectoryCounter;
    }

    private int trajectoryCounter = 0;


    public void setPartitionID(Integer partition) {
        this.partitionID = partition;
    }

    public int getHorizontalTrieID() {
        return horizontalTrieID;
    }


    public Node getRoot() {
        return root;
    }

    public void insertTrajectory2(List<Integer> roadSegments, int trajectoryID, long startingTime, long endingTime) {
        Node currentNode=root, child = root;
        int previousRoadSegment = -1;

        for (int i = 0; i < roadSegments.size(); i++) {
            int roadSegment = roadSegments.get(i);
            if (roadSegment == previousRoadSegment) {
                continue;
            }
            currentNode = child;
// when i==0 currentNode = child = root
            child = currentNode.getChildren(roadSegment);

            if (child == null) {
                child = currentNode.addChild(roadSegment);

            }

            previousRoadSegment = roadSegment;
        }
//        child.addTrajectory(endingTime, trajectoryID);
        child.addStartingTime(startingTime,trajectoryID);
        child.addEndingTime(endingTime,trajectoryID);


    }


//    public void insertTrajectory2(List<Integer> roadSegments, int trajectoryID, long startingTime, long endingTime) {
//        Node currentNode, child = root;
//        int previousRoadSegment = -1;
//
//        for (int i = 0; i < roadSegments.size(); i++) {
//            int roadSegment = roadSegments.get(i);
//            if (roadSegment == previousRoadSegment) {
//                continue;
//            }
//            currentNode = child;
//// when i==0 currentNode = child = root
//            child = currentNode.getChildren(roadSegment);
//
//            if (child == null) {
//                child = currentNode.addChild(roadSegment);
//
//            }
//
//            previousRoadSegment = roadSegment;
//        }
////        child.addTrajectory(endingTime, trajectoryID);
//        child.addStartingTime(startingTime,trajectoryID);
//        child.addEndingTime(endingTime,trajectoryID);
//
//
//    }
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
     * This method does not answer strict path queries
     * If query is ABCD, it returns trajectories that have only passed through AB for example
     *
     * @param q
     * @return
     */
    public Set<Integer> queryIndex(Query q) {

        Node currentNode = root;
        Set<Integer> answer = new TreeSet<>();

        for (int i = 0; i < q.getPathSegments().size(); i++) {

            int roadSegment = q.getPathSegments().get(i);


            if (currentNode.getRoadSegment() == roadSegment) {
                //stay on this node, query roadSegment is repeated
                continue;
            }
            Node child = currentNode.getChildren(roadSegment);

            if (child == null) {
                //no matching result
                break;
            } else {
                //filter time here
                answer.addAll(child.getTrajectories(q.getStartingTime(), (q.getEndingTime() + 1)));
                currentNode = child;
            }
        }

        return answer;
    }

    public double avgIndexDepth() {

        Node currentNode = root;

        LinkedList<Node> queue = new LinkedList<>();
        queue.push(currentNode);

        int depth = 0;
        int counter = 0;
        int sum = 0;

        while (!queue.isEmpty()) {

            currentNode = queue.pop();
            List<Node> allChildren = currentNode.getAllChildren();

            if (allChildren.isEmpty()) {
                sum += depth;
                ++counter;
                depth = 0;
            }
            ++depth;

            queue.addAll(allChildren);

        }

        double avgBranchDepth = (double) sum / counter;

        return avgBranchDepth;

//        System.exit(1);
    }


    public double avgIndexWidth() {

        Node currentNode = root;

        LinkedList<Node> queue = new LinkedList<>();
        queue.add(currentNode);

        int counter = 0;
        int sum = 0;

        while (!queue.isEmpty()) {

            currentNode = queue.remove();
            List<Node> allChildren = currentNode.getAllChildren();

            sum += currentNode.getAllChildren2().size();
            ++counter;

            queue.addAll(allChildren);

        }

        double avgBranchWidth = (double) sum / counter;

        return avgBranchWidth;

//        System.exit(1);
    }


    public void setHorizontalTrieID(int horizontalTrieID) {
        this.horizontalTrieID = horizontalTrieID;
    }

    @Override
    public String toString() {
        return "TriePrint{" +
                "root=" + root.getRoadSegment() +
                ", horizontalTrieID=" + horizontalTrieID +
                '}';
    }

    public  void callthis(){
        System.out.println("nof children:"+getRoot().getAllChildren().size());

    }

    public void insertTrajectory2(Trajectory traj) {
        ++trajectoryCounter;


        if (traj.getStartingTime()<=minStartingTime) {
            minStartingTime = traj.getStartingTime();
        }

        if (traj.getStartingTime()>=maxStartingTime) {
            maxStartingTime = traj.getStartingTime();
        }
//
        if (traj.getRoadSegments().size()<=minTrajLength) {
            minTrajLength = traj.getRoadSegments().size();
        }

        if (traj.getRoadSegments().size()>=maxTrajLength) {
            maxTrajLength = traj.getRoadSegments().size();
        }

        if (traj.getStartingRS()<=minStartingRS) {
            minStartingRS = traj.getStartingRS();
        }

        if (traj.getStartingRS()>=maxStartingRS) {
            maxStartingRS = traj.getStartingRS();
        }


        insertTrajectory2(traj.roadSegments, traj.trajectoryID, traj.getStartingTime(), traj.getEndingTime());
    }


    public boolean checkStartingRS(int startingRoadSegment) {

        return root.checkIfRSExists(startingRoadSegment);
//        return allStartingRSegments.contains(startingRoadSegment);
    }

    public void setHilbertValue(Long hilbertValue) {
        this.hilbertValue = hilbertValue;
    }
}
