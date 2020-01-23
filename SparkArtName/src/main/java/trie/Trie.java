package trie;


import comparators.IntegerComparator;
import comparators.LongComparator;
import utilities.Trajectory;

import java.io.Serializable;
import java.util.*;

public class Trie implements Serializable {
    private Node root = Node.getNode(Integer.MAX_VALUE);

    private long minTrajLength = Long.MAX_VALUE;
    private long maxTrajLength = Long.MIN_VALUE;

    private long minStartingTime = Long.MAX_VALUE;
    private long maxStartingTime = Long.MIN_VALUE;
    private int minStartingRS = Integer.MAX_VALUE;

    public int getMinStartingRS() {
        return minStartingRS;
    }

    public int getMaxStartingRS() {
        return maxStartingRS;
    }

    private int maxStartingRS = Integer.MIN_VALUE;


    public long getMinStartingTime() {
        return minStartingTime;
    }

    public long getMaxStartingTime() {
        return maxStartingTime;
    }


    public int partitionID;
    public void setPartitionID(Integer partition) {
        this.partitionID = partition;
    }


    public void insertTrajectory2(List<Integer> roadSegments, int trajectoryID, List<Long> times) {
        Node currentNode = root, child = root;
        int previousRoadSegment = -1;


        for (int i = 0; i < roadSegments.size(); i++) {
            int roadSegment = roadSegments.get(i);
            if (roadSegment == previousRoadSegment) {
                child.addStartingTime(times.get(i), trajectoryID);
                continue;
            }
            currentNode = child;
            ArrayList<Node> currentNodeList = currentNode.getChildren(roadSegment);
            if (currentNodeList==null){
                child=null;
            }
            else{
                child = currentNode.getChildren(roadSegment).get(new Random().nextInt(currentNodeList.size()));
            }


            if (child == null) {
                child = currentNode.addChild(roadSegment);
                root.addToRoot(roadSegment, child);
            }

            child.addStartingTime(times.get(i), trajectoryID);

            previousRoadSegment = roadSegment;
        }
    }


    public Set<Integer> queryIndex(Query q) {

        int level=0;
        return queryNode2(root,q,q.getStartingTime(),q.getEndingTime(),level);
//        return queryNode(root,q.getStartingRoadSegment(),q.getStartingTime(),q.getEndingTime());
    }

    public Set<Integer> queryNode2(Node n, Query query, long startingTime, long endingTime, int level) {

        Set<Integer> answer = new TreeSet<>();

        if (n.getRoadSegment()!=query.getPathSegments().get(level)){
            System.err.println("wtf");
            System.exit(1);
        }
        Queue<Node> queue = new LinkedList<>();
        answer.addAll(n.getTrajectories(startingTime, endingTime+1));
        if (level>=query.getPathSegments().size()){
            return answer;
        }
        ArrayList<Node> nodesChildren =n.getChildren(query.getPathSegments().get(level));
        if (nodesChildren==null){
//            return answer;
            nodesChildren=new ArrayList<Node>();
        }
        queue.addAll(nodesChildren);


        while (!queue.isEmpty()){
            Node peekedNode = queue.poll();
            answer.addAll(queryNode2(peekedNode,query, startingTime,endingTime,level+1));
        }
        return answer;
    }

    public Set<Integer> queryNode(Node n, Integer currentRoadSegment, long startingTime, long endingTime) {

        Set<Integer> answer = new TreeSet<>();

        Queue<Node> queue = new LinkedList<>();
        answer.addAll(n.getTrajectories(startingTime, endingTime+1));
        ArrayList<Node> nodesChildren =n.getChildren(currentRoadSegment);
        if (nodesChildren==null){
            return answer;
        }
        queue.addAll(nodesChildren);


        while (!queue.isEmpty()){
            Node peekedNode = queue.poll();
            answer.addAll(queryNode(peekedNode,peekedNode.getRoadSegment(), startingTime,endingTime));
        }
        return answer;
    }

//    public Set<Integer> queryIndex(Query q) {
//
//        Node currentNode = root;
//        Set<Integer> answer = new TreeSet<>();
//
////        Queue<Node> queue = new LinkedList<>();
////
////        queue.addAll(root.getChildren(q.getStartingRoadSegment()));
////
////        while (!queue.isEmpty()) {
////
////        }
//        List<Node> allasd=root.getAllChildren();
//
////        for (Node n:allasd) {
////            System.out.println("::"+n.getRoadSegment());
////        }
////        System.exit(1);
//
//        for (int i = 0; i < q.getPathSegments().size(); i++) {
//
//            int roadSegment = q.getPathSegments().get(i);
//
//
//            if (currentNode.getRoadSegment() == roadSegment) {
//                //stay on this node, query roadSegment is repeated
//                continue;
//            }
//            ArrayList<Node> allChildren = currentNode.getChildren(roadSegment);
//            if (allChildren == null) {
//                //no matching result
//                break;
//            } else {
//                for (Node child : allChildren) {
//                    Collection<Integer> tids = child.getTrajectories(q.getStartingTime(), (q.getEndingTime() + 1));
//                    if (!tids.isEmpty()) {
//                        currentNode = child;
//                        //filter time here
//                        answer.addAll(tids);
//                    }
//                }
//
////
//            }
//        }
//
//        return answer;
//    }


    public void insertTrajectory2(Trajectory traj) {


        int minRs=traj.getRoadSegments().stream().min(new IntegerComparator()).get();
        int maxRs=traj.getRoadSegments().stream().max(new IntegerComparator()).get();
        long maxTime=traj.getTimestamps().stream().max(new LongComparator()).get();


        if (traj.getStartingTime() <= minStartingTime) {
            minStartingTime = traj.getStartingTime();
        }

        if (maxTime >= maxStartingTime) {
            maxStartingTime = maxTime;
        }
//
        if (traj.getRoadSegments().size() <= minTrajLength) {
            minTrajLength = traj.getRoadSegments().size();
        }

        if (traj.getRoadSegments().size() >= maxTrajLength) {
            maxTrajLength = traj.getRoadSegments().size();
        }

        if ( minRs<= minStartingRS) {
            minStartingRS = minRs;
        }

        if (maxRs >= maxStartingRS) {
            maxStartingRS = maxRs;
        }


        insertTrajectory2(traj.roadSegments, traj.trajectoryID, traj.timestamps);
    }

}
