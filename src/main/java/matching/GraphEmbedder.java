package matching;

public class GraphEmbedder {

    private GraphMatcher m = new GraphMatcher();

    public GraphEmbedder() {
        // TODO Auto-generated constructor stub
    }

    public GraphEmbedder(GraphMatcher m) {
        this.m = m;
    }

    public void setNodeFields(String... fields) {
        m.addSignatureNodeField(fields);
    }

    public void setEdgeFields(String... fields) {
        m.addSignatureEdgeField(fields);
    }

    public double distance(IdGraph idg1, IdGraph idg2) throws MatchException {
        return m.distance(idg1, idg2);
    }

    public double[][] distanceMatrix(IdGraph[] graphs) throws MatchException {

        double[][] answer = new double[graphs.length][graphs.length];
        for (int i = 0; i < graphs.length; i++) {
            for (int j = 0; j < graphs.length; j++) {
                answer[i][j] = m.distance(graphs[i], graphs[j]);
            }
        }

        return answer;
    }
}
