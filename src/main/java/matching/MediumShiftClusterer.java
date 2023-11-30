package matching;

import data.DataException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.Graph;
import data.InvalidFieldException;
import data.InvalidKeyException;
import data.InvalidTupleException;
import data.KeyCollisionException;
import data.NonExistingTupleException;
import data.Pair;
import data.Schema;
import data.Table;
import data.Tuple;

public class MediumShiftClusterer<T> implements Clusterer<T> {

    static String CENTER = "center";
    static String NEIGHBOR = "neighbor";
    static String DISTANCE = "distance";
    static String KEY = "key";
    Weiszfeld<T> weiszfeld = new Weiszfeld<>();
    protected Matcher<T> m;

    public MediumShiftClusterer(Matcher<T> matcher) {
        this.m = matcher;
    }

    @Override
    public Graph clusterizeAsGraph(T[] objects, double radius, float precision) {

        Table nodes = new Table();
        nodes.getSchema().addField("object", objects[0].getClass());
        nodes.getSchema().setKeyByColumns("object", "object");
        Graph gms = new Graph(nodes, false);
        Table edges = gms.getEdgesTable();
        edges.getSchema().addField("distance", Double.class);

        if (objects.length > 0) {

            Table neighbors = calculateNeighbors(objects, radius);
            Tuple tgi = null;
            Tuple tgm = null;
            Tuple edge;
            T median = null;
            Pair<T, Double> pair;
            T old = null;
            boolean notConverged = true;


            for (T gi : objects) {
                
                median = gi;
                pair = new Pair<>(gi, 0d);
                do {
                    //acha todos grafos cuja distancia de gi é menor que o raio
                    Table nearBy;
                    try {
                        nearBy = neighbors.filterEquals(NEIGHBOR, median);
                        if (nearBy.size() > 0) {
                            pair = weiszfeld.getMedianElement(nearBy, precision);
                            median = pair.getFirst();
                            if (median == old) {
                                notConverged = false;
                                break;
                            }
                        } else {
                            notConverged = false;
                            break;
                        }

                        old = median;

                    } catch (InvalidFieldException e) {
                        e.printStackTrace();
                        break;
                    }



                } while (notConverged);

                try {
                    tgm = gms.getNodesTable().searchRow("object", median);
                    if (tgm == null) {
                        tgm = gms.addNode(median);
                    }
                    tgi = gms.getNodesTable().searchRow("object", gi);
                    if (tgi == null) {
                        tgi = gms.addNode(gi);
                    }
                    Collection<Tuple> out = gms.getOutNodes(tgm);
//                    Collection<Tuple> out = gms.getOutLinks(tgm);
                    if (out.size() > 0) {
                        tgm = out.iterator().next();
                    }
                    
                    edge = gms.addEdge(tgi, tgm);
//                    edge.set(DISTANCE, weiszfeld.lastCalculatedDistance);
                    edge.set(DISTANCE, pair.getSecond());

                } catch (InvalidTupleException | InvalidFieldException | InvalidKeyException | KeyCollisionException | NonExistingTupleException e) {
                    throw new DataException(e);
                }

            }
        }
        return gms;
    }

    @Override
    public Map<T, List<T>> clusterize(T[] objects, double radius, float precision) {

        Map<T, List<T>> gms = new HashMap<>();
        if (objects.length > 0) {

            Table neighbors = this.calculateNeighbors(objects, radius);

            T median = null;
            T old = null;
            Pair<T, Double> pair;
            boolean notConverged = true;


            for (T gi : objects) {
                median = gi;
                pair = new Pair<>(gi, 0d);
                do {
                    //acha todos grafos cuja distancia de gi é menor que o raio
                    //gm = mediana(Gi);
                    Table nearBy;
                    try {
                        nearBy = neighbors.filterEquals(NEIGHBOR, median);//.filterSmallerOrEquals(distance, radius);
                        if (nearBy.size() > 0) {
                            pair = weiszfeld.getMedianElement(nearBy, precision);//.get(0);
                            median = pair.getFirst();
                            if (median == old) {
                                notConverged = false;
                                break;
                            }
                        } else {
                            notConverged = false;
                            break;
                        }

                        old = median;

                    } catch (InvalidFieldException e) {
                        e.printStackTrace();
                        break;
                    }



                } while (notConverged);
                if (!gms.containsKey(median)) {
                    gms.put(median, new ArrayList<>());
                }
                gms.get(median).add(gi);

            }
        }
        return gms;
    }

    private Table calculateNeighbors(T[] objects, double radius) {
        Table neighbors = new Table();
        Schema s = neighbors.getSchema();
        s.addField(CENTER, objects[0].getClass());
        s.addField(NEIGHBOR, objects[0].getClass());
        s.addField(DISTANCE, Double.class, Double.MAX_VALUE);
        s.setKeyByColumns(KEY, CENTER, NEIGHBOR);



        long antes = System.currentTimeMillis();
        System.err.println("	Beginning to calculate the neighbors ");
        ///////////////////////////f
        T object, neighbor;
        double distance;
        for (int i = 0; i < objects.length; i++) {
            object = objects[i];
            try {
                neighbors.addRow(object, object, 0d);

                for (int j = i + 1; j < objects.length; j++) {
                    neighbor = objects[j];
                    distance = 0;

                    distance = m.distance(object, neighbor);
                    if (distance < radius) {
                        neighbors.addRow(object, neighbor, distance);
                        neighbors.addRow(neighbor, object, distance);

                    }
                }

            } catch (InvalidTupleException | InvalidFieldException | MatchException | KeyCollisionException e1) {
                e1.printStackTrace();
            }
        }
        System.err.println("	Finished calculating the neighbors = " + (System.currentTimeMillis() - antes) / 1000.0);
        return neighbors;
    }
}
