package matching;

import data.InvalidFieldException;
import data.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import data.Table;

//Weiszfeld's algorithm 
public class Weiszfeld<T> {

    public Pair<T, Double> getMedianElement(Table input, float precision) throws InvalidFieldException {

        T answer = null;
        Set<T> centers = input.<T>getUniqueValues(MediumShiftClusterer.CENTER);

        double minDist = Double.MAX_VALUE;

        List<Double> listaDistancias;
        for (T idg : centers) { //Calcula mediana para uma dimensao de cada vez
            listaDistancias = new ArrayList<>();
            listaDistancias.addAll(input.<Double>getValuesAsCollection(MediumShiftClusterer.DISTANCE));

            double d = geometricMedian(listaDistancias, precision);
            if (d < minDist) {
                minDist = d;
                answer = idg;
            }


        }
        double dist = ((int) (minDist * 100)) / 100d;
        System.err.println("            "+dist);
//        this.lastCalculatedDistance = dist;
        return new Pair<>(answer, dist);

    }

    static public double geometricMedian(List<Double> input, float precision) {
        double m0 = mean(input);
        double m1 = recursiveMedian(m0, input);
        while (Math.abs(m1 - m0) > precision) {
            m0 = m1;
            m1 = recursiveMedian(m0, input);
        }
        return m1;
    }

    static private double mean(List<Double> input) {
        double soma = 0d;
        //System.out.println("input = "+input);
        for (double d : input) {
            soma += d;
        }
        return soma / input.size();
    }

    static private double recursiveMedian(double m0, List<Double> x) {
        double m1;
        double numerator = 0;
        double denominator = 0;
        for (int i = 0; i < x.size(); i++) {
            double temp = Math.pow((x.get(i) - m0), 2);
            if (temp != 0) {
                numerator += x.get(i) / temp;
                denominator += 1 / temp;
            } else {
                return 0;
            }
            //System.out.println(numerator/denominator);
        }
        m1 = numerator / denominator;
        return m1;
    }
}
