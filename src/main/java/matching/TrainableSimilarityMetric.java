package matching;

import java.util.List;

public interface TrainableSimilarityMetric extends SimilarityMetric {

    void train(List<Object> objs);
}
