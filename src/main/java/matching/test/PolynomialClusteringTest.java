/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package matching.test;

import data.Graph;
import data.Tuple;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import matching.Matcher;
import matching.MediumShiftClusterer;

/**
 *
 * @author caio
 */
public class PolynomialClusteringTest {

    public static void main(String[] args) {
        
        
        List<Double> pol1 = Arrays.asList(2d, 1d, 3d);
        List<Double> pol2 = Arrays.asList(2d, 1d, 0d);
        List<Double> pol3 = Arrays.asList(1d, 1d, 3d);
        List<Double> pol4 = Arrays.asList(0d, 1d, 3d);
        List<Double> pol5 = Arrays.asList(0d, 8d, 3d);
        List<Double> pol6 = Arrays.asList(0d, 1d, 2d);
        
        Matcher<List<Double>> matcher = (List<Double> object1, List<Double> object2) -> {
            double diff0 = (Math.pow(object1.get(2), 0)-Math.pow(object2.get(2), 0));
            double diff1 = (Math.pow(object1.get(1), 1)-Math.pow(object2.get(1), 1));
            double diff2 = (Math.pow(object1.get(0), 2)-Math.pow(object2.get(0), 2));
            return diff0+diff1+diff2;

//            double diff0 = (Math.pow(object1.get(2)-object2.get(2), 0));
//            double diff1 = (Math.pow(object1.get(1)-object2.get(1), 1));
//            double diff2 = (Math.pow(object1.get(0)-object2.get(0), 2));
//            return diff0+diff1+diff2;
        };
        
        MediumShiftClusterer msc = new MediumShiftClusterer(matcher);
        List[] polynoms = {pol1, pol2, pol3, pol4, pol5, pol6};
        Map<List<Double>, List<List<Double>>> clusterized = msc.clusterize(polynoms, 1d, 0.15f);
        
        clusterized.keySet().stream().map((centroid) -> {
            System.err.println(centroid);
            return centroid;
        }).forEachOrdered((centroid) -> {
            clusterized.get(centroid).forEach((neighbor) -> {
                System.err.println("        "+neighbor);
            });
        });
        
        
        Graph g  = msc.clusterizeAsGraph(polynoms, 2d, 0.15f);
        for(Tuple t : g.getNodesTable().listTuples()){
            System.err.println("TUPLE "+t);
            System.err.println("        "+g.getOutLinks(t));
//            System.err.println("        "+g.getInLinks(t));
//            System.err.println("           "+g.g);
        }
        System.err.println(g.getEdgesTable());
        

    }
    
}
