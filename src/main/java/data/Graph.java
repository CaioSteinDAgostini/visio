package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Graph implements Serializable {

    private Table nodes;
    private Table edges;
    public static final String SOURCE = "source";
    public static final String TARGET = "target";
    public static final String EDGE_ID = "edgeId";
    private final Boolean multiEdge;

    public Graph(boolean isMultiEdge) {
        this.nodes = new Table();
        this.edges = new Table();
        this.multiEdge = isMultiEdge;

        edges.getSchema().addField(SOURCE, Tuple.class);
        edges.getSchema().addField(TARGET, Tuple.class);

        if (!isMultiEdge) {

            edges.getSchema().setKeyByColumns(EDGE_ID, SOURCE, TARGET);

        } else {
            edges.addIndex(SOURCE);
            edges.addIndex(TARGET);
        }
    }

    public Graph(final Table nodes, boolean isMultiEdge) {
        this.nodes = nodes;
        this.edges = new Table();
        this.multiEdge = isMultiEdge;

        edges.getSchema().addField(SOURCE, Tuple.class);
        edges.getSchema().addField(TARGET, Tuple.class);

        if (!isMultiEdge) {

            edges.getSchema().setKeyByColumns(EDGE_ID, SOURCE, TARGET);

        } else {
            edges.addIndex(SOURCE);
            edges.addIndex(TARGET);
        }
    }

    private Graph(Table nodes, Table edges, boolean isMultiEdge) {
        this.nodes = nodes;
        this.edges = edges;
        this.multiEdge = isMultiEdge;

        edges.getSchema().addField(SOURCE, Tuple.class);
        //edges.addIndex(SOURCE);
        edges.getSchema().addField(TARGET, Tuple.class);
        //edges.addIndex(TARGET);
        try {
            if (!isMultiEdge) {

                edges.getSchema().setKeyByColumns(EDGE_ID, SOURCE, TARGET);

            } else {
                edges.addIndex(SOURCE);
                edges.addIndex(TARGET);
            }
        } catch (InvalidFieldException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void reset(){
        this.nodes.reset();
        this.edges.reset();
    }
    
    public boolean addNodeProperty(String name, Class type, boolean isIndexed, Object fillingValue) {
        Schema s = nodes.getSchema();
        if (s.hasField(name)) {
            return false;
        } else {
            try {
                s.addField(name, type, fillingValue);
                if (isIndexed) {
                    if (Comparable.class.isAssignableFrom(type)) {
                        nodes.addIndex(name);
                    } else {
                        throw new InvalidFieldException(type + "  is not Comparable");
                    }
                }
                nodes.getColumn(name).reset();
            } catch (InvalidFieldException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }

    public boolean removeNodeProperty(String name) {
        if (nodes.getSchema().hasField(name)) {
            nodes.getSchema().removeField(name);
            return true;
        }
        return false;
    }

    public boolean addEdgeProperty(String name, Class type, boolean isIndexed, Object fillingValue) {

        Schema s = edges.getSchema();
        if (s.hasField(name)) {
            return false;
        } else {
            try {
                s.addField(name, type, fillingValue);
                if (isIndexed) {
                    edges.addIndex(name);
                }
                edges.getColumn(name).reset();
            } catch (InvalidFieldException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }

    public boolean removeEdgeProperty(String name) {
        if (edges.getSchema().hasField(name)) {
            edges.getSchema().removeField(name);
            return true;
        }
        return false;
    }

    public String getEdgeSourceField() {
        return SOURCE;
    }

    public String getEdgeTargetField() {
        return TARGET;
    }

    public Table getNodesTable() {
        return nodes;
    }

    public Table getEdgesTable() {
        return edges;
    }

    public Tuple addNode(Object... values) throws InvalidTupleException, KeyCollisionException {

        try {
            return nodes.addRow(values);
        } catch (InvalidFieldException e) {
            throw new InvalidTupleException(e);
        }
    }

    public Tuple addEdge(final Tuple source, final Tuple target) {
        if (source.getTable().equals(target.getTable())) {
            Tuple answer;
            answer = edges.addRow(source, target);

            return answer;
        } else {
            throw new NonExistingTupleException();
        }

    }

    public void removeEdge(final Tuple edge) {
        this.edges.removeRow(edge);
    }

    public void removeNode(final Tuple node) {
        nodes.removeRow(node);
        Table temp;
        Iterator<Tuple> it;

        temp = edges.filterEquals(SOURCE, node);
        it = temp.iterator();
        while (it.hasNext()) {
            edges.removeRow(it.next());
        }

        temp = edges.filterEquals(TARGET, node);
        it = temp.iterator();
        while (it.hasNext()) {
            edges.removeRow(it.next());
        }

    }

    public Tuple getEdgeSource(final Tuple edge) {
        return edge.get(this.getEdgeSourceField());
    }

    public Tuple getEdgeTarget(final Tuple edge) {
        return edge.get(this.getEdgeTargetField());
    }

    public List<Tuple> getInLinks(final Tuple node) throws InvalidTupleException {

        Table t;
        t = edges.filterEquals(TARGET, node);
        return t.stream().collect(Collectors.toList());
    }

    public List<Tuple> getInNodes(final Tuple node) throws InvalidTupleException {

        Table t;
        t = edges.filterEquals(TARGET, node);
        return t.stream().map((tuple) -> {
            return tuple.<Tuple>get(SOURCE);
        }).collect(Collectors.toList());
    }

    public List<Tuple> getOutLinks(final Tuple node) throws InvalidTupleException {

        Table t;
        t = edges.filterEquals(SOURCE, node);
        return t.stream().collect(Collectors.toList());
    }

    public List<Tuple> getOutNodes(final Tuple node) throws InvalidTupleException {

        Table t;
        t = edges.filterEquals(SOURCE, node);
        return t.stream().map((tuple) -> {
            return tuple.<Tuple>get(TARGET);
        }).collect(Collectors.toList());

    }

    public Iterator<Tuple> getFromToEdges(final Tuple from, final Tuple to) throws InvalidTupleException, InvalidFieldException {

        Table filtered = edges.filterEquals(SOURCE, from).filterEquals(TARGET, to);
        return filtered.iterator();
    }

    public Tuple getEdge(int row) {
        return this.edges.getRow(row);
    }

    public Tuple getNode(int row) {
        return this.nodes.getRow(row);
    }

    public Integer size() {
        return this.getNodesTable().size();
    }

    public Integer edgeSize() {
        return this.getEdgesTable().size();
    }

    public Graph setNodeKeyByColumns(String keyName, String... fieldsNames) throws InvalidFieldException {
        this.nodes.getSchema().setKeyByColumns(keyName, fieldsNames);
        return this;
    }

    public Tuple searchNode(String keyname, Object... values) throws InvalidTupleException, InvalidKeyException {
        return this.nodes.searchRow(keyname, values);
    }

    public List<Tuple> breadthFirstSearch(Tuple start, Tuple end) {
        return breadthFirstSearch(start, end, Set.of());
    }

    public List<Tuple> breadthFirstSearch(Tuple start, Tuple end, Set<Tuple> toIgnore) {
        String TUPLE = "TUPLE";
        Graph tracking = new Graph(false);
        tracking.addNodeProperty(TUPLE, Tuple.class, false, null);
        Set<Tuple> closed = new HashSet<>();
        LinkedList<Tuple> open = new LinkedList<>();
        closed.add(start);
        open.add(start);

        boolean found = false;
        while ((!open.isEmpty()) && (!found)) {
            // Dequeue a vertex from queue and print it
            Tuple node = open.poll();
            tracking.addNode(node);
            List<Tuple> nextNodes = this.getOutNodes(node);
            for (Tuple next : nextNodes) {
                if (!toIgnore.contains(next)) {
                    tracking.addEdge(node, next);
                    if (next.equals(end)) {
                        found = true;
                        break;
                    }
                    if (!closed.contains(next)) {
                        closed.add(next);
                        open.add(next);
                    }
                }
            }
        }

        if (!found) {
            return List.of();
        }

        List<Tuple> answer = new LinkedList<>();
        answer.add(end);
        boolean finished = false;
        Tuple t = end;
        do {
            List<Tuple> before = tracking.getInNodes(t);
            tracking.removeNode(t);
            if (!before.isEmpty()) {
                t = before.get(0);
                answer.add(t);
                if (t.equals(start)) {
                    finished = true;
                }
            } else {
                finished = true;
            }
        } while (!finished);
        Collections.reverse(answer);
        return answer;
    }

    public static void main(String args[]) {
        Graph g = new Graph(false);
        g.addNodeProperty("INT", Integer.class, true, null);
        Tuple t0 = g.addNode(0);
        Tuple t1 = g.addNode(1);
        Tuple t2 = g.addNode(2);
        Tuple t3 = g.addNode(3);
        Tuple t4 = g.addNode(4);

        g.addEdge(t0, t1);
        g.addEdge(t0, t2);
        g.addEdge(t1, t2);
//        g.addEdge(t2, t0);
        g.addEdge(t2, t3);
//        g.addEdge(t3, t3);
        g.addEdge(t3, t4);
        g.addEdge(t4, t0);

        Tuple start = t0;
        Tuple end = t0;
        g.breadthFirstSearch(t0, end).forEach((tuple) -> {
            System.err.println(tuple);
        });

    }
}
