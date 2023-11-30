package matching;

import java.util.List;
import java.util.Map;

import data.Graph;

public interface Clusterer<T> {

    Map<T, List<T>> clusterize(T[] objects, double radius, float precision);

    Graph clusterizeAsGraph(T[] objects, double radius, float precision);
}
