package matching;

public interface Matcher<T> {

    double distance(T object1, T object2) throws MatchException;
}
