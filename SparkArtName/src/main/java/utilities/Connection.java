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
        return (o instanceof Connection) && getDestination().getWord()==((Connection) o).getDestination().getWord();
    }

    @Override
    public int hashCode(){
//        System.out.println("hashCode called");

        return Long.hashCode(destination.resolve().getWord());
    }
    public Node getDestination() {
        return destination.resolve();
    }

    public Connection(Node destination) {
        this.destination = new NodeRef(destination);
    }

    private  NodeRef destination;

}
