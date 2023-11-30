package matching;

public class LevenshteinDistance implements SimilarityMetric {

    @Override
    public double similarityDegree(Object obj1, Object obj2) {

        String s1 = (String) obj1;
        String s2 = (String) obj2;

        int n = s1.length();
        int m = s2.length();

        if (n == 0) {
            return m;
        }
        if (m == 0) {
            return n;
        }

        int[][] d = new int[n + 1][m + 1];

        for (int i = 0; i <= n; d[i][0] = i++);
        for (int j = 1; j <= m; d[0][j] = j++);

        for (int i = 1; i <= n; i++) {
            char sc = s1.charAt(i - 1);
            for (int j = 1; j <= m; j++) {
                int v = d[i - 1][j - 1];
                if (s2.charAt(j - 1) != sc) {
                    v++;
                }
                d[i][j] = Math.min(Math.min(d[i - 1][ j] + 1, d[i][j - 1] + 1), v);
            }
        }
        return d[n][m];

    }
}
