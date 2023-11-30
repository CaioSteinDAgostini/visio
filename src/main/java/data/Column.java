package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Column implements Serializable {

    private String name;
    private ArrayList rows;
    private Class type;
    private Boolean indexed;
    private Index idx;
    private Object defaultValue;
    protected ReentrantLock lock;

    public Column(final String name, final Class type, boolean indexed) {
        this.type = type;
        this.rows = new ArrayList();
        this.name = name;
        this.indexed = indexed;
        //this.defaultValue = null;
        if (indexed) {
            idx = new Index(this);
        }
        this.lock = new ReentrantLock();
    }

    public Column(final String name, final Class type) {
        this.type = type;
        this.rows = new ArrayList();
        this.name = name;
        this.indexed = false;
        //this.defaultValue = null;
        this.lock = new ReentrantLock();
    }

    public Boolean hasIndex() {
        return idx != null;
    }

    public Column createIndex() {
        idx = new Index(this);
        this.indexed = true;
        return this;
    }

    public Column removeIndex() {
        idx = null;         //new Index(this);
        this.indexed = false;        //true;
        return this;
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return this.type;
    }

    public Object get(int row) {
        return type.cast(rows.get(row));
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    public Column setDefaultValue(Object value) {
        this.defaultValue = value;
        return this;
    }

    public Column set(int row, final Object value) throws InvalidFieldException {
        lock.lock();
        try {
            if (value == null || type.isAssignableFrom(value.getClass())) {
                while (row >= rows.size()) {
                    rows.add(this.defaultValue);
                }
                rows.set(row, value);
                if (indexed) {
                    idx.insert(row, value);
                }
            } else {
                throw new InvalidFieldException("Should be " + this.type + " but is " + value.getClass());
            }
            return this;
        } finally {
            lock.unlock();
        }
    }

    public Column remove(int row) {
        lock.lock();
        try {
            if (indexed) {
                idx.remove(row, this.get(row));
            }
//            else{
//                this.rows.remove(row);
//            }
            this.rows.trimToSize();
            return this;
        } finally {
            lock.unlock();
        }
    }

    public Column clear() {
        lock.lock();
        try {
            this.rows.clear();
            if (indexed) {
                idx.clear();
            }
            this.rows.trimToSize();
            return this;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return rows.toString();
    }

    public <T> Collection<T> getRowsAsCollection() {
        return new ArrayList<>(this.rows);
    }

    public final Iterator<Integer> hasEquals(final Object value) {
        return this.hasEqualsAsCollection(value).iterator();

    }

    public Collection<Integer> hasEqualsAsCollection(final Object value) {
        Collection<Integer> answer = new ArrayList<>();
        if (indexed) {
            answer = idx.getEquals(value);
        } else {
            //answer = new ArrayList<Integer>();
            Iterator it = this.rows.iterator();
            Object o;
            int i = 0;
            while (it.hasNext()) {
                o = it.next();
                if (o != null && value != null) {
                    if (type.cast(o).equals(type.cast(value))) {
                        answer.add(i);
                    }
                } else {
                    if (o == null && value == null) {
                        answer.add(i);
                    }
                }
                i++;
            }
        }
        return answer;
    }

    public Collection<Integer> hasDifferentAsCollection(final Object value) {
        Collection<Integer> answer = new ArrayList<>();
        if (indexed) {
            answer = idx.getDifferent(value);
        } else {
            //answer = new ArrayList<Integer>();
            Iterator it = this.rows.iterator();
            Object o;
            int i = 0;
            while (it.hasNext()) {
                o = it.next();
                if (o != null && value == null) {
                    answer.add(i);
                } else {
                    if (o != null && (!type.cast(o).equals(type.cast(value)))) {
                        answer.add(i);
                    }

                }

                i++;
            }
        }
        return answer;
    }

    public Iterator<Integer> hasDifferent(final Object value) {
        return this.hasDifferentAsCollection(value).iterator();

    }

    public Collection<Integer> hasLargerOrEqualsAsCollection(final Comparable value) {
        if (indexed) {
            return idx.getLargerOrEqual(value);
        } else {
            return IntStream.range(0, rows.size()).parallel().boxed().map(
                    index -> new Pair<>(index, this.rows.get(index))
            ).filter(pair
                    -> (pair.getSecond() != null && ((Comparable) pair.getSecond()).compareTo(value) >= 0)
            ).map(pair -> pair.getFirst()).collect(Collectors.toList());
        }
    }

    public Iterator<Integer> hasLargerOrEquals(final Comparable value) {
        return this.hasLargerOrEqualsAsCollection(value).iterator();

    }

    public Collection<Integer> hasLargerThanAsCollection(final Comparable value) {
        if (indexed) {
            return idx.getLargerThan(value);
        } else {
            return IntStream.range(0, rows.size()).parallel().boxed().map(
                    index -> new Pair<>(index, this.rows.get(index))
            ).filter(pair
                    -> (pair.getSecond() != null && ((Comparable) pair.getSecond()).compareTo(value) > 0)
            ).map(pair -> pair.getFirst()).collect(Collectors.toList());
        }
    }

    public Iterator<Integer> hasLargerThan(final Comparable value) {
        return this.hasLargerThanAsCollection(value).iterator();

    }

    public Collection<Integer> hasSmallerOrEqualsAsCollection(final Comparable value) {
        if (indexed) {
            return idx.getSmallerOrEqual(value);
        } else {
            return IntStream.range(0, rows.size()).parallel().boxed().map(
                    index -> new Pair<>(index, this.rows.get(index))
            ).filter(pair
                    -> (pair.getSecond() != null && ((Comparable) pair.getSecond()).compareTo(value) <= 0)
            ).map(pair -> pair.getFirst()).collect(Collectors.toList());
        }
    }

    public Iterator<Integer> hasSmallerOrEquals(final Comparable value) {
        return this.hasSmallerOrEqualsAsCollection(value).iterator();

    }

    public Collection<Integer> hasSmallerThanAsCollection(final Comparable value) {
        if (indexed) {
            return idx.getSmallerThan(value);
        } else {
            return IntStream.range(0, rows.size()).parallel().boxed().map(
                    index -> new Pair<>(index, this.rows.get(index))
            ).filter(pair
                    -> (pair.getSecond() != null && ((Comparable) pair.getSecond()).compareTo(value) < 0)
            ).map(pair -> pair.getFirst()).collect(Collectors.toList());
        }
    }

    public Iterator<Integer> hasSmallerThan(final Comparable value) {
        return this.hasSmallerThanAsCollection(value).iterator();

    }

    public <T> Set<T> getUniqueValues() {
        lock.lock();
        try {
            Set answer = new HashSet();
            boolean hasIndex = idx != null;
            if (!hasIndex) {
                for (Object o : rows) {
                    if (o != null) {
                        answer.add(o);
                    }
                }
            } else {
                answer = idx.getUniqueValues();
            }
            return answer;
        } finally {
            lock.unlock();
        }
    }

    public <T> T max() {
        lock.lock();
        try {
            T answer;
            boolean hasIndex = idx != null;
            if (!hasIndex) {
                this.createIndex();
            }
            answer = idx.max();
            if (!hasIndex) {
                this.removeIndex();
            }
            return answer;
        } finally {
            lock.unlock();
        }
    }

    public <T> T min() {
        lock.lock();
        try {
            T answer;
            boolean hasIndex = idx != null;
            if (!hasIndex) {
                this.createIndex();
            }
            answer = idx.min();
            if (!hasIndex) {
                this.removeIndex();
            }
            return answer;
        } finally {
            lock.unlock();
        }
    }

    public Column reset() {
        lock.lock();
        try {
            for (int i = 0; i < rows.size(); i++) {
                rows.set(i, this.defaultValue);
            }
            return this;
        } finally {
            lock.unlock();
        }
    }
}
