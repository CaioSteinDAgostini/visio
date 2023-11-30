package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Index implements Serializable {

    private final TreeMap ts;
    private final Column field;
    //private String isUnique;
    private String name;
    private Collection l;
    private final List<Integer> nullValues;

    public Index(final Column field) {

        this.ts = new TreeMap();
        this.field = field;
        this.nullValues = new ArrayList<>();
        l = field.getRowsAsCollection(); 
        Set<Integer> indexesList;
        int i = 0;
        for (Object o : l) {
            if (o == null) {
                nullValues.add(i);
            } else {
                if (!ts.containsKey(o)) {
                    indexesList = new HashSet<>();
                    ts.put(o, indexesList);
                } else {
                    indexesList = (Set<Integer>) ts.get(o);
                }
                indexesList.add(i);
            }
            i++;

//ts.put(o, index);
//l.set(i, null);
        }
        l = null;
    }

    public void insert(int index, final Object value) {
        Set<Integer> indexesList;
        if (value == null) {
            nullValues.add(index);
        } else {
            if (!ts.containsKey(value)) {
                indexesList = new HashSet<>();
                ts.put(value, indexesList);
            } else {
                indexesList = (Set<Integer>) ts.get(value);
            }
            indexesList.add(index);
        }
        //
    }

    public void remove(int index, final Object value) {
        if (value == null) {
            nullValues.remove(index);
        } else {
            if (ts.containsKey(value)) {
                Set<Integer> indexesList = (Set<Integer>) ts.get(value);
                indexesList.remove(index);
//                indexesList.remove(value);
                if (indexesList.size() == 0) {
                    ts.remove(value);
                }
            }
        }
    }
    
    public Index clear(){
        ts.clear();
        nullValues.clear();
        return this;
    }

    public List<Integer> getLargerThan(final Comparable value) {
        List<Integer> answer = new ArrayList();
        Object larger = ts.higherKey(value);
        while (larger != null) {
            answer.addAll((Collection<? extends Integer>) ts.get(larger));
            larger = ts.higherKey(larger);
        }
        return answer;
    }

    public List<Integer> getLargerOrEqual(final Comparable value) {
        List<Integer> answer = this.getLargerThan(value);
        if (ts.containsKey(value)) {
            answer.addAll((Collection<? extends Integer>) ts.get(value));
        }
        return answer;
    }

    public List<Integer> getSmallerThan(final Comparable value) {
        List<Integer> answer = new ArrayList();
        Object smaller = ts.lowerKey(value);
        while (smaller != null) {
            answer.addAll((Collection<? extends Integer>) ts.get(smaller));
            smaller = ts.lowerKey(smaller);
        }
        return answer;
    }

    public List<Integer> getSmallerOrEqual(final Comparable value) {
        List<Integer> answer = this.getSmallerThan(value);
        if (ts.containsKey(value)) {
            answer.addAll((Collection<? extends Integer>) ts.get(value));
        }
        return answer;
    }

    public List<Integer> getEquals(final Object value) {
        List<Integer> answer = new ArrayList<>();
        if (value != null && ts.containsKey(value)) {
            answer.addAll((Collection<? extends Integer>) ts.get(value));
        }
        return answer;
    }

    public boolean hasEqual(final Object value) {
        return ts.containsKey(value);
    }

    public Collection<Integer> getDifferent(Object value) {
        List<Integer> answer = new ArrayList<>();
        answer.addAll(this.ts.values());
        answer.removeAll(this.getEquals(value));
        return answer;
    }

    public Set<Object> getUniqueValues() {
        return ts.keySet();
    }

    public <T> T  max() {
        if (ts.size() > 0) {
            return (T) ts.lastKey();
        } else {
            return null;
        }
    }

    public <T> T  min() {
        if (ts.size() > 0) {
            return (T) ts.firstKey();
        } else {
            return null;
        }
    }
}
