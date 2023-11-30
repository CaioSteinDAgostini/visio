package matching;

public class DoubleMatcher implements Matcher<Double> {

    @Override
    public double distance(Double o1, Double o2) {
        return Math.abs(o1 - o2);
    }
}
