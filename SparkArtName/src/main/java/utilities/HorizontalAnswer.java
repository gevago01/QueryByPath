package utilities;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by giannis on 25/03/19.
 */
public class HorizontalAnswer implements Serializable{

    private int queryID;
    private TreeSet<Integer> answer=new TreeSet<>();

    @Override
    public String toString() {
        return "HorizontalAnswer{" +
                "queryID=" + queryID +
                ", answer=" + answer +
                '}';
    }

    public TreeSet<Integer> getAnswer() {
        return answer;
    }



    public HorizontalAnswer(int queryID){
        this.queryID=queryID;
    }

    public void addTrajIDs(Set<Integer> ans) {
        answer.addAll(ans);

    }


}
