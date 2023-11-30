package matching;

import data.Graph;

public class IdGraph {

    final public Graph g;
    final public int id;

    public IdGraph(Graph g, int id) {
        this.g = g;
        this.id = id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    static public IdGraph[] wrap(Graph[] graphs) {
        IdGraph[] answer = new IdGraph[graphs.length];
        int i = 0;
        for (Graph g : graphs) {
            answer[i] = new IdGraph(g, i++);
        }
        return answer;
    }

    @Override
    public String toString() {
        //return id+"";
        return g.getNodesTable().toString() + g.getEdgesTable().toString();
    }
}
