package utilities;

import trie.Node;

import java.io.Serializable;

/**
 * Created by giannis on 22/12/18.
 */
public class Connection implements Serializable {


    @Override
    public boolean equals(Object o)
    {
        return (o instanceof Connection) && getDestination().getRoadSegment()==((Connection) o).getDestination().getRoadSegment();
    }


    @Override
    public int hashCode(){
        return Integer.hashCode(destination.resolve().getRoadSegment());
    }
    public Node getDestination() {
        return destination.resolve();
    }

    public Connection(Node destination) {
        this.destination = new NodeRef(destination);
    }

    private  NodeRef destination;

}
