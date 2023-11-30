package matching.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

import matching.DoubleMatcher;
import matching.GraphMatcher;
import matching.IdGraph;
import matching.Matcher;
import matching.MediumShiftClusterer;
import data.Graph;
import data.InvalidFieldException;
import data.InvalidTupleException;
import data.KeyCollisionException;
import data.NonExistingTupleException;
import data.Schema;
import data.Table;
import data.Tuple;

public class ClusteringTest {

    public static void main(String[] args) throws NonExistingTupleException, InvalidTupleException, InvalidFieldException, KeyCollisionException {

        String id = "id";
        String weight = "weight";
        Schema s = new Schema();
        s.addField(id, String.class);
        s.addField(weight, Double.class);
        s.setKeyByColumns("key", id);

        Table nt1 = new Table(s);
        Graph g1 = new Graph(nt1, false);
        Tuple g1n0 = g1.addNode("google labs", 10d);
        Tuple g1n1 = g1.addNode("google research", 44d);
        Tuple g1n2 = g1.addNode("microsoft", 9d);
        Tuple g1n3 = g1.addNode("microsafe", 2d);
        Tuple g1n4 = g1.addNode("IBM research", 13d);
        Tuple g1n5 = g1.addNode("yahoo research", 33d);
        g1.addEdge(g1n0, g1n3);
        g1.addEdge(g1n4, g1n3);
        g1.addEdge(g1n1, g1n2);
        g1.addEdge(g1n2, g1n5);

        Table nt2 = new Table(s);
        Graph g2 = new Graph(nt2, false);
        Tuple g2n0 = g2.addNode("google lab", 15d);
        Tuple g2n1 = g2.addNode("google research lab", 22d);
        Tuple g2n2 = g2.addNode("microsoft", 14d);
        Tuple g2n3 = g2.addNode("microsoft labs", 24d);
        //Tuple g2n4 = g2.addNode(4, 13);
        //Tuple g2n5 = g2.addNode(5, 33);
        g2.addEdge(g2n0, g2n3);
        //g1.addEdge(g2n4, g2n3);
        g2.addEdge(g2n1, g2n2);
        //g1.addEdge(g2n2, g2n5);
        g2.addEdge(g2n2, g2n0);


        Table nt3 = new Table(s);
        Graph g3 = new Graph(nt3, false);
        //Tuple g3n0 = g3.addNode(0, 10);
        Tuple g3n1 = g3.addNode("IBM labs", 234d);
        Tuple g3n2 = g3.addNode("IBM research", 29d);
        Tuple g3n3 = g3.addNode("yahoo labs", 24d);
        Tuple g3n4 = g3.addNode("yahoo research", 134d);
        Tuple g3n5 = g3.addNode("google", 3d);
        g3.addEdge(g3n1, g3n3);
        g3.addEdge(g3n3, g3n2);
        g3.addEdge(g3n1, g3n4);
        g3.addEdge(g3n4, g3n5);

        Table nt4 = new Table(s);
        Graph g4 = new Graph(nt4, false);
        Tuple g4n0 = g4.addNode("IBM laboratories", 2d);
        Tuple g4n1 = g4.addNode("google laboratories", 14d);
        Tuple g4n2 = g4.addNode("yahoo laboratories", 4d);
        Tuple g4n3 = g4.addNode("microsoft laboratories", 54d);
        Tuple g4n4 = g4.addNode("microsafe laboratories", 1d);
        Tuple g4n5 = g4.addNode("intel labs", 43d);
        g4.addEdge(g4n3, g4n4);
        g4.addEdge(g4n0, g4n3);
        g4.addEdge(g4n1, g4n2);
        g4.addEdge(g4n2, g4n5);


        Matcher<IdGraph> m = new GraphMatcher();
        ((GraphMatcher) m).addSignatureNodeField(weight);
        MediumShiftClusterer<IdGraph> c = new MediumShiftClusterer<>(m);
        Graph[] graphs = {g1, g2, g3, g4};
        //Graph cg = c.clusterizeAsGraph(IdGraph.wrap(graphs), 5, 0.05f);

        Double[] doubles = {99d, 1d, 2d, 3d, 1000d, 1005d};
        Matcher<Double> dm = new DoubleMatcher();
        MediumShiftClusterer<Double> msc = new MediumShiftClusterer<>(dm);
        Graph cg = msc.clusterizeAsGraph(doubles, 150, 0.015f);


    }

    public Serializable loadElements(InputStream input) throws IOException, ClassNotFoundException {
        Serializable result;

        ObjectInputStream objectInput = new ObjectInputStream(input);
        result = (Serializable) objectInput.readObject();
        return result;
    }
}
