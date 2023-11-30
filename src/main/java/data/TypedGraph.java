///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
//package data.event;
//
//import data.Graph;
//import static data.Graph.SOURCE;
//import static data.Graph.TARGET;
//import data.InvalidTupleException;
//import data.KeyCollisionException;
//import data.Table;
//import data.Tuple;
//import java.security.InvalidParameterException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.Optional;
//
///**
// *
// * @author caio
// * @param <N>
// * @param <E>
// */
//public class TypedGraph<N extends Comparable, E extends Comparable> {
//    
//    final Graph g;
//    String NODE_FIELD = "NODE";
//    String EDGE_FIELD = "EDGE";
//
//    public TypedGraph(Class<N> nodeClass, Class<E> edgeClass, boolean isMultiEdge){
//        this.g = new Graph( isMultiEdge);
//        g.addNodeProperty(NODE_FIELD, nodeClass, true, null);
//        g.addEdgeProperty(EDGE_FIELD, edgeClass, true, null);
//    }
//    
//    
//    public TypedGraph<N, E> addNode(N node) throws InvalidTupleException, KeyCollisionException {
//        Tuple answer = g.addNode(node);
//        return this;
//    }
//
//    public TypedGraph<N, E> addEdge(N source, E edge, N target) {
//        synchronized (g) {
//            if (!g.getNodesTable().contains(EDGE_FIELD, edge)) {
//                Tuple sourceTuple = g.searchNode(NODE_FIELD, source);
//                Tuple targetTuple = g.searchNode(NODE_FIELD, target);
//                Tuple edgeTuple = g.addEdge(sourceTuple, targetTuple);
//                edgeTuple.set(EDGE_FIELD, edge);
//
//                return this;
//            } else {
//                throw new InvalidParameterException();
//            }
//        }
//    }
//
//    public TypedGraph<N, E> removeEdge(E edge)  {
//        synchronized (g){
//            g.getEdgesTable().searchRowAndRemove(EDGE_FIELD, edge);
//            return this;
//        }
//    }
//
//    public TypedGraph<N, E> removeNode(N node) {
//        synchronized(g){
//            Tuple nodeTuple = g.searchNode(NODE_FIELD, node);
//            if(nodeTuple!=null){
//                g.removeNode(nodeTuple);
//            }
//            return this;
//        }
//    }
//
//    public N getEdgeSource(E edge) {
//        Tuple edgeTuple = g.getEdgesTable().searchRow(EDGE_FIELD, edge);
//        if(edgeTuple!=null){
//            return g.getEdgeSource(edgeTuple).get(NODE_FIELD);
//        }
//        throw new InvalidParameterException();
//    }
//
//    public N getEdgeTarget(E edge) {
//        Tuple edgeTuple = g.getEdgesTable().searchRow(EDGE_FIELD, edge);
//        if(edgeTuple!=null){
//            return g.getEdgeTarget(edgeTuple).get(NODE_FIELD);
//        }
//        throw new InvalidParameterException();
//    }
//
//    public Collection<Tuple> getInLinks(final Tuple node) throws InvalidTupleException {
//
//        Collection<Tuple> answer = new ArrayList<>();
//        Table t;
//            t = edges.filterEquals(TARGET, node);
//            Iterator<Tuple> it = t.iterator();
//
//            while (it.hasNext()) {
//                answer.add(it.next());
//            }
//        return answer;
//
//    }
//
//    public Collection<Tuple> getInNodes(final Tuple node) throws InvalidTupleException {
//
//        Collection<Tuple> answer = new ArrayList<>();
//        Table t;
//            t = edges.filterEquals(TARGET, node);
//            Iterator<Tuple> it = t.iterator();
//
//            while (it.hasNext()) {
//                answer.add(it.next().<Tuple>get(SOURCE));
//            }
//        return answer;
//
//    }
//
//    public Collection<Tuple> getOutLinks(final Tuple node) throws InvalidTupleException {
//
//        Collection<Tuple> answer = new ArrayList<>();
//        Table t;
//            t = edges.filterEquals(SOURCE, node);
//            Iterator<Tuple> it = t.iterator();
//
//
//            while (it.hasNext()) {
//                answer.add(it.next());
//            }
//        return answer;
//
//    }
//
//    public Collection<Tuple> getOutNodes(final Tuple node) throws InvalidTupleException {
//
//        Collection<Tuple> answer = new ArrayList<>();
//        Table t;
//            t = edges.filterEquals(SOURCE, node);
//            Iterator<Tuple> it = t.iterator();
//
//
//            while (it.hasNext()) {
//                answer.add(it.next().<Tuple>get(TARGET));
//            }
//        return answer;
//
//    }
//}
