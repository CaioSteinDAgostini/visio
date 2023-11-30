package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author cstein
 *
 */
public class Table implements Serializable {

    private Map<String, Column> columns;
    private SortedSet<Integer> freeRows;
    private Integer lastRow;
    public Map<Integer, Tuple> tuples;
    final private Schema schema;
    ReentrantLock columnsLock;

    public Table() {
        this.schema = new Schema();
        this.init();
    }

    public Table(Schema schema) {
        this.init();
        this.schema = schema;
        Iterator<String> fields = schema.getFields();
        String field;
        while (fields.hasNext()) {
            field = fields.next();
            this.addColumn(field);
        }
    }

    public Table(Table cloned) {

        columnsLock = new ReentrantLock();
        columns = new HashMap<>(cloned.columns);
        freeRows = new TreeSet<>(cloned.freeRows);
        tuples = new HashMap<>(cloned.tuples);
        schema = cloned.schema;
        lastRow = cloned.lastRow;
    }

    public Schema getSchema() {
        return this.schema;
    }

    private void init() {
        tuples = new HashMap<>();
        columns = new LinkedHashMap<>();
        freeRows = new TreeSet<>();
        lastRow = -1;
        columnsLock = new ReentrantLock();

    }

    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    public Iterator<Tuple> iterator() {
        return this.tuples.values().iterator();
    }

    public boolean validateWithSchema(String field) {
        try {
            columnsLock.lock();
            boolean answer = false;
            boolean schemaContains = this.schema.hasField(field);
            boolean dontHave = !this.columns.containsKey(field) && !schemaContains;
            boolean have = false;
            Column c = this.columns.get(field);
            Class fieldType = this.schema.getFieldType(field);
            if (c != null && fieldType != null) {
                have = fieldType.equals(c.getType());
            }
            if (dontHave || have) {
                answer = true;
            }
            return answer;
        } finally {
            columnsLock.unlock();
        }
    }

    public Table removeUnusedColumns() {
        try {
            columnsLock.lock();
            for (String s : this.columns.keySet()) {
                if (!this.getSchema().hasField(s)) {
                    columns.remove(s);
                }
            }
            return this;
        } finally {
            columnsLock.unlock();
        }
    }

    private Table addColumn(String field) {
        try {
            columnsLock.lock();

            if (this.schema.hasField(field)) {
                Column c = new Column(field, schema.getFieldType(field));
                c.setDefaultValue(schema.getDefaultValue(field));
                columns.put(field, c);
                Iterator<Tuple> it = this.iterator();
                while (it.hasNext()) {
                    try {
                        this.set(it.next(), field, c.getDefaultValue());
                    } catch (InvalidTupleException | InvalidFieldException | KeyCollisionException e) {
                        e.printStackTrace();
                    }
                }
            }
            return this;
        } finally {
            columnsLock.unlock();
        }
    }

    public Table addIndex(String field) throws InvalidFieldException {
        try {
            columnsLock.lock();

            this.getColumn(field).createIndex();
            return this;
        } finally {
            columnsLock.unlock();
        }
    }

    public Table removeIndex(String field) throws InvalidFieldException {
        try {
            columnsLock.lock();

            this.getColumn(field).removeIndex();
            return this;
        } finally {
            columnsLock.unlock();
        }
    }

    public Column getColumn(String field) throws InvalidFieldException {
        try {
            columnsLock.lock();

            return this.getColumn(schema.getIndexOf(field));
        } finally {
            columnsLock.unlock();
        }
    }

    public Column getColumn(Integer index) {
        try {
            columnsLock.lock();
            String f = schema.getField(index);
            Column answer = null;

            if ((!columns.containsKey(f))) {
                this.addColumn(f);
            }
            answer = columns.get(f);

            if (!this.validateWithSchema(answer.getName())) {
                this.removeColumn(answer.getName());
                answer = null;
            }
            return answer;
        } finally {
            columnsLock.unlock();
        }
    }

    private void removeColumn(String field) {
        try {
            columnsLock.lock();
            if (columns.containsKey(field)) {
                columns.remove(field);
            }
        } finally {
            columnsLock.unlock();
        }
    }

    protected void referenceRow(Tuple tuple) throws InvalidFieldException {
        if (tuple != null) {
            this.tuples.put(tuple.getRow(), tuple);
            for (String field : columns.keySet()) {
                columns.get(field).set(tuple.getRow(), tuple.get(field));
            }
        }
    }

    public Table cloneRows(Table table) throws InvalidFieldException {
        if (this.getSchema().equals(table.getSchema())) {
            table.stream().forEach((tuple) -> {
                this.cloneRow(tuple);
            });
            return this;
        } else {
            throw new InvalidFieldException();
        }
    }

    public Tuple cloneRow(Tuple toClone) throws InvalidFieldException {
        int size = this.getSchema().getSize();

        Object[] values = new Object[size];
        int i = 0;

        HashMap<String, Object> mappedValues = new HashMap<>();
        String field;
        while (i < size) {
            field = schema.getField(i);
            mappedValues.put(field, toClone.get(field));
            i++;
        }

        Iterator<String> kit = schema.getKeys();
        while (kit.hasNext()) {
            String key = kit.next();
            Object[] keyValues = new Object[schema.getKeySize(key)];
            Iterator<String> fields = schema.getKeyFields(key);
            i = 0;
            while (fields.hasNext()) {
                keyValues[i] = mappedValues.get(fields.next());
                i++;
            }
            try {
                Tuple t = this.searchRow(key, keyValues);
                if (t != null) {
                    throw new InvalidTupleException(t.toString());
                }
            } catch (InvalidKeyException e) {
                throw new InvalidTupleException();
            }

        }

        Tuple tuple = new Tuple();

        tuple.setTable(this);
        tuple.setRow(this.nextRow());
        Iterator<String> fields = this.getSchema().getFields();

        while (fields.hasNext()) {
            field = fields.next();
            try {
                tuple.setUnchecked(field, mappedValues.get(field));
            } catch (InvalidFieldException e) {
                e.printStackTrace();
            }
        }
        tuple.setValid(true);
        this.tuples.put(tuple.getRow(), tuple);

        return tuple;
    }

    public Tuple addDefaultRow() throws InvalidTupleException {
        int size = this.getSchema().getSize();

        Object[] values = new Object[size];
        int i = 0;

        HashMap<String, Object> mappedValues = new HashMap<>();
        String field;
        while (i < size) {
            field = schema.getField(i);
            mappedValues.put(field, schema.getDefaultValue(field));
            i++;
        }

        Iterator<String> kit = schema.getKeys();
        while (kit.hasNext()) {
            String key = kit.next();
            Object[] keyValues = new Object[schema.getKeySize(key)];
            Iterator<String> fields = schema.getKeyFields(key);
            i = 0;
            while (fields.hasNext()) {
                keyValues[i] = mappedValues.get(fields.next());
                i++;
            }
            try {
                Tuple t = this.searchRow(key, keyValues);
                if (t != null) {
                    throw new InvalidTupleException(t.toString());
                }
            } catch (InvalidKeyException e) {
                throw new InvalidTupleException();
            }

        }

        Tuple tuple = new Tuple();

        tuple.setTable(this);
        tuple.setRow(this.nextRow());
        Iterator<String> fields = this.getSchema().getFields();

        while (fields.hasNext()) {
            field = fields.next();
            try {
                tuple.setUnchecked(field, mappedValues.get(field));
            } catch (InvalidFieldException e) {
                e.printStackTrace();
            }
        }
        tuple.setValid(true);
        this.tuples.put(tuple.getRow(), tuple);
        //////////////
        ///////////////
        //////////

        return tuple;
    }

    private Object[] getKey(String key, final Object... values) {

        int keySize = this.getSchema().getKeySize(key);
        int schemaSize = this.getSchema().getSize();
        if (values.length < keySize || values.length > schemaSize) {
            throw new DataException();
        }

        Object[] answer = schema.getKeyFieldsStream(key).map((field) -> {
            return schema.getIndexOf(field);
        }).map((index) -> {
            return values[index];
        }).toArray();

        return answer;
    }

    public Tuple addRow(final Object... values) throws InvalidTupleException, InvalidFieldException, KeyCollisionException {

        final Table table = this;
        int size = this.getSchema().getSize();

        Object[] newValues = Arrays.copyOf(values, size);
        if (values.length < size) {
            for (int i = values.length; i < size; i++) {
                newValues[i] = getSchema().getDefaultValue(i);
            }
        }

        if (newValues.length == size) {

            HashMap<String, Object> mappedValues = new HashMap<>();
            IntStream.range(0, schema.getSize()).boxed().forEach((t) -> {
                mappedValues.put(schema.getField(t), newValues[t]);
            });

            boolean collides = schema.getKeysNamesStream().anyMatch((key) -> {
                return table.searchRow(key, getKey(key, newValues)) != null;
            });
            if (collides) {
                throw new KeyCollisionException("key collision");
            }

            Tuple tuple = new Tuple();
            tuple.setTable(this);
            tuple.setRow(this.nextRow());
            this.getSchema().getFieldsStream().forEach((field) -> {
                tuple.setUnchecked(field, mappedValues.get(field));
            });

            tuple.setValid(true);
            this.tuples.put(tuple.getRow(), tuple);

            return tuple;
        } else {
            throw new InvalidTupleException();
        }

    }

    public Table removeValidRows(Table table) throws InvalidTupleException {
        List<Tuple> toRemove = table.listTuples().stream().sorted((t, t1) -> {
            return Double.compare(t.getRow(), t1.getRow()); //in opposite order to sort reversed;
        }).collect(Collectors.toList());
        Collections.reverse(toRemove);
        toRemove.forEach((tuple) -> {
            if (tuple.isValid()) {
                this.freeRows.add(tuple.getRow());
                this.tuples.remove(tuple.getRow());
                for (Column c : columns.values()) {
                    c.remove(tuple.getRow());
                }
                tuple.setValid(false);
            } else {
//                throw new InvalidTupleException();
            }
        });

        return this;
    }

    public Table removeRow(Tuple tuple) throws InvalidTupleException {
        if (tuple.isValid()) {
            this.freeRows.add(tuple.getRow());
            tuples.remove(tuple.getRow());
            for (Column c : columns.values()) {
                c.remove(tuple.getRow());
            }
            tuple.setValid(false);
        }
        return this;
    }

    public Tuple getRow(int row) {
        return tuples.get(row);
    }

    public Object get(int row, String field) throws InvalidFieldException {
        Object answer = null;
        try {
            answer = columns.get(field).get(row);
        } catch (NullPointerException e) {
            throw new InvalidFieldException(field);
        }
        return answer;
    }

    public boolean contains(String keyName, Object... values) throws InvalidTupleException, InvalidKeyException {
        return this.searchRow(keyName, values) != null;
    }

    public Table searchRowAndRemove(String keyName, Object... values) throws InvalidTupleException, InvalidKeyException {
        Tuple tuple = this.searchRow(keyName, values);
        if (tuple != null) {
            this.removeRow(tuple);
        }
        return this;
    }

    public Tuple searchRow(String keyName, Object... values) throws InvalidKeyException {
        if (this.schema.getKeySize(keyName) == values.length) {
            Optional<Collection<Integer>> ids = IntStream.range(0, schema.getKeySize(keyName)).parallel().boxed().map(
                    fieldIndex -> new Pair<>(this.getSchema().getKeyField(keyName, fieldIndex), values[fieldIndex])
            ).map(
                    pair -> {
                        Collection<Integer> indexes = this.getColumn(pair.getFirst()).hasEqualsAsCollection(pair.getSecond());
                        return indexes;
                    }
            ).reduce(
                    (pair1, pair2) -> {
                        pair1.retainAll(pair2);
                        return pair1;
                    }
            );

            if (ids.isPresent()) {
                if (ids.get().size() == 1) {
                    return this.getRow(ids.get().iterator().next());
                } else {
                    return null;
                }
            } else {
                throw new InvalidKeyException(keyName);
            }
        } else {
            throw new InvalidKeyException(keyName);
        }
    }

    public Boolean hasEquals(String fieldName, Object value) throws InvalidFieldException {
        Column c = this.getColumn(fieldName);
        return c.hasEqualsAsCollection(value).size() > 0;
    }

    public Table filterEquals(String fieldName, final Object... values) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);
        if (c == null) {
            throw new InvalidFieldException();
        }
        Iterator<Integer> indexIterator;
        if (values == null) {
            throw new NullPointerException();
//                values = new Object[]{null};
        }
        for (Object v : values) {
            indexIterator = c.hasEquals(v);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }

        }

        return answer;
    }

    public Table filterDifferent(String fieldName, Object... values) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);
        if (c == null) {
            throw new InvalidFieldException();
        }
        Iterator<Integer> indexIterator;
        if (values == null) {
            throw new NullPointerException();
//                values = new Object[]{null};
        }
        for (Object v : values) {
            indexIterator = c.hasDifferent(v);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }

        }

        return answer;
    }

    public Table filterNull(String fieldName) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);
        if (c == null) {
            throw new InvalidFieldException();
        } else {
            Iterator<Integer> indexIterator;
            indexIterator = c.hasEquals(null);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }
        }
        return answer;
    }

    public Table filterNotNull(String fieldName) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);
        if (c == null) {
            throw new InvalidFieldException();
        } else {
            Iterator<Integer> indexIterator;
            indexIterator = c.hasDifferent(null);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }
        }
        return answer;
    }

    public Table filterLargerThan(String fieldName, Comparable... values) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);

        Iterator<Integer> indexIterator;
        for (Comparable v : values) {
            indexIterator = c.hasLargerThan(v);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }

        }
        return answer;
    }

    public Table filterLargerOrEquals(String fieldName, Comparable... values) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);

        Iterator<Integer> indexIterator;
        for (Comparable v : values) {
            indexIterator = c.hasLargerOrEquals(v);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }

        }
        return answer;
    }

    public Table filterSmallerThan(String fieldName, Comparable... values) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);

        Iterator<Integer> indexIterator;
        for (Comparable v : values) {
            indexIterator = c.hasSmallerThan(v);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }

        }
        return answer;
    }

    public Table filterSmallerOrEquals(String fieldName, Comparable... values) throws InvalidFieldException {
        Table answer = new Table(this.getSchema());
        Column c = this.getColumn(fieldName);

        Iterator<Integer> indexIterator;
        for (Comparable v : values) {
            indexIterator = c.hasSmallerOrEquals(v);
            while (indexIterator.hasNext()) {
                Tuple t = this.getRow(indexIterator.next());
                answer.referenceRow(t);
            }

        }
        return answer;
    }

    public int nextRow() {
        int answer = ++lastRow;
        if (freeRows.size() > 0) {
            int first = freeRows.first();
            if (first <= lastRow) {
                freeRows.remove(first);
                answer = first;
            }
        }
        return answer;
    }

    public int currentRow() {
        return lastRow;
    }

    @Override
    public String toString() {
        Iterator<Tuple> it = this.iterator();
        String answer = "\n---------------------------\n" + this.schema + "\nsize = " + tuples.size() + "\n";
        while (it.hasNext()) {
            answer = answer + it.next() + "\n";
        }
        answer = answer + "---------------------------\n";
        return answer;
    }

    public int numberOfColumns() {
        return this.columns.size();
    }

    public int size() {
        return this.tuples.size();
    }

    public Iterator<String> getFieldsNames() {
        return this.schema.getFields();
    }

    public Table set(Tuple tuple, String field, Object value) throws InvalidTupleException, InvalidFieldException, KeyCollisionException {
        this.set(tuple.getRow(), field, value);
        return this;
    }

    public Table set(int row, String field, Object value) throws InvalidFieldException, KeyCollisionException {

        //se tiver o mesmo valor na coluna sendo setada, pega os indices que tem esses valores.
        //depois, pra cada outro campo, verifica se eles tem os valores que estao setados pra tupla do row sendo editado... guarda todos indices
        //faz um retainsAll entre todas colecoes e, se sobrar algo, � pq tem colisao
        Iterator<String> keys = this.schema.isPartOfKeys(field);
        String key;
        Iterator<String> keyFieldsIterator;
        Column keyedColumn;
        String keyField;
        Class type;

        int colided = 0;
        Tuple potentialColision;
        Tuple newTuple = this.getRow(row);
        while (keys.hasNext()) {
            key = keys.next();
            keyFieldsIterator = schema.getKeyFields(key); ///o campo faz parte de todas essas chaves!

            keyedColumn = this.getColumn(field); //pego a coluna pra o campo em questao e vejo se alguem ja tem o mesmo valor
            Collection<Integer> tuplesIndexes = keyedColumn.hasEqualsAsCollection(value); //vendo se ja tem o mesmo valor. Se nao tem, ja nao tem nem vai ter colisao
            if (tuplesIndexes.size() > 0) { //para cada um dos indices que ja colide NESTE campo em questao
                //tem que verificar se, os outros campos destas colisoes, tbm colidem com os campos da tupla "row"

                for (int i : tuplesIndexes) {
                    potentialColision = this.getRow(i);
                    if (potentialColision != null) {
                        colided = 0;
                        while (keyFieldsIterator.hasNext()) {
                            keyField = keyFieldsIterator.next();
                            type = schema.getFieldType(keyField);

                            if (potentialColision.get(keyField) != null && newTuple.get(keyField) != null) {
                                if ((type.cast(potentialColision.get(keyField))).equals(type.cast(newTuple.get(keyField)))) {
                                    colided++;
                                } else {
                                    break;
                                }
                            } else if (potentialColision.get(keyField) == null && newTuple.get(keyField) == null) {
                                colided++;
                            } else {
                                break;
                            }
                        }
                        if (colided + 1 == schema.getKeySize(key)) {
                            throw new KeyCollisionException(key);
                        }
                    }
                }
            }
        }
        if (!columns.containsKey(field)) {
            this.addColumn(field);
        }
        Column column = columns.get(field);//getColumn(field);

        column.set(row, value);

        return this;

    }

    public Table resume(String indexField, String resumedField) throws InvalidFieldException {
        Table resumedTable = new Table();
        Schema rs = resumedTable.getSchema();

        rs.addField(indexField, this.schema.getFieldType(indexField));
        rs.addField(resumedField, ArrayList.class);

        Column c = this.getColumn(indexField);
        Set<Object> uniqueValues = c.getUniqueValues();

        for (Object v : uniqueValues) {
            Collection col = c.hasEqualsAsCollection(v);
            resumedTable.addRow(v, col);
        }

        return resumedTable;
    }

    public <T> T max(String field) throws InvalidFieldException {
        return this.getColumn(field).max();
    }

    public <T> T min(String field) throws InvalidFieldException {
        return this.getColumn(field).min();
    }

    public Table reset() {
        for (Column c : columns.values()) {
            c.clear();
        }
        this.tuples.clear();
        this.freeRows.clear();
        this.lastRow = -1;
        return this;
    }

    public Table reset(String field) throws InvalidFieldException {
        this.getColumn(field).reset();
        return this;
    }

    public <T> Set<T> getUniqueValues(String field) throws InvalidFieldException {
        return this.getColumn(field).getUniqueValues();
    }

    public <T> List<T> getValuesAsCollection(String field) throws InvalidFieldException {
        return this.tuples.values().stream().map(t -> {
            return t.<T>get(field);
        }).collect(Collectors.toList());
    }

    public List<Tuple> sort(String field) throws InvalidFieldException {

        final Class<? extends Comparable> cl = this.getSchema().getFieldType(field);

        List answer = new ArrayList<>(this.tuples.values());
        Collections.sort(answer, (Tuple o1, Tuple o2) -> {
            try {
                Comparable c1 = cl.cast(o1.get(field));;
                Comparable c2 = cl.cast(o2.get(field));
                return c1.compareTo(c2);
            } catch (InvalidFieldException ex) {
                return 0;
            }
        });
        return answer;
    }

    public List<Tuple> listTuples() {
        List<Tuple> answer = new ArrayList<>(this.tuples.values());
        return answer;
    }

    public List<Tuple> replaceIfEquals(String fieldToCompare, Object valueToCompare, String fieldToReplace, Object valueToReplace, boolean removeIfCollision) throws InvalidFieldException, KeyCollisionException {
        //listTuples to not have concurrentModification
        return this.filterEquals(fieldToCompare, valueToCompare).listTuples().stream().map((tuple) -> {
            try {
                tuple.set(fieldToReplace, valueToReplace);
            } catch (KeyCollisionException e) {
                if (removeIfCollision) {
                    this.removeRow(tuple);
                    return null;
                } else {
                    throw e;
                }
            }
            return tuple;
        }).filter((tuple) -> {
            return tuple != null;
        }).collect(Collectors.toList());
    }

    public List<Tuple> replaceIfDifferent(String fieldToCompare, Object valueToCompare, String fieldToReplace, Object valueToReplace, boolean removeIfCollision) throws InvalidFieldException, KeyCollisionException {
        //listTuples to not have concurrentModification
        return this.filterDifferent(fieldToCompare, valueToCompare).listTuples().stream().map((tuple) -> {
            try {
                tuple.set(fieldToReplace, valueToReplace);
            } catch (KeyCollisionException e) {
                if (removeIfCollision) {
                    this.removeRow(tuple);
                    return null;
                } else {
                    throw e;
                }
            }
            return tuple;
        }).filter((tuple) -> {
            return tuple != null;
        }).collect(Collectors.toList());
    }

    public List<Tuple> replaceIfNull(String fieldToCompare, String fieldToReplace, Object valueToReplace, boolean removeIfCollision) throws InvalidFieldException, KeyCollisionException {
        //listTuples to not have concurrentModification
        return this.filterNull(fieldToCompare).listTuples().stream().map((tuple) -> {
            try {
                tuple.set(fieldToReplace, valueToReplace);
            } catch (KeyCollisionException e) {
                if (removeIfCollision) {
                    this.removeRow(tuple);
                    return null;
                } else {
                    throw e;
                }
            }
            return tuple;
        }).filter((tuple) -> {
            return tuple != null;
        }).collect(Collectors.toList());
    }

    public List<Tuple> replaceIfNotNull(String fieldToCompare, String fieldToReplace, Object valueToReplace, boolean removeIfCollision) throws InvalidFieldException, KeyCollisionException {
        //listTuples to not have concurrentModification
        return this.filterNotNull(fieldToCompare).listTuples().stream().map((tuple) -> {
            try {
                tuple.set(fieldToReplace, valueToReplace);
            } catch (KeyCollisionException e) {
                if (removeIfCollision) {
                    this.removeRow(tuple);
                    return null;
                } else {
                    throw e;
                }
            }
            return tuple;
        }).filter((tuple) -> {
            return tuple != null;
        }).collect(Collectors.toList());
    }

    public Table removeIfEquals(String fieldToCompare, Object valueToCompare) throws InvalidFieldException, KeyCollisionException, InvalidTupleException {
        //listTuples to not have concurrentModification
        this.filterEquals(fieldToCompare, valueToCompare).listTuples().stream().forEach((tuple) -> {
            this.removeRow(tuple);
        });
        return this;
    }

    public Table removeIfDifferent(String fieldToCompare, Object valueToCompare) throws InvalidFieldException, KeyCollisionException, InvalidTupleException {
        //listTuples to not have concurrentModification
        this.filterDifferent(fieldToCompare, valueToCompare).listTuples().stream().forEach((tuple) -> {
            this.removeRow(tuple);
        });
        return this;
    }

    public Table removeIfNull(String fieldToCompare) throws InvalidFieldException, KeyCollisionException, InvalidTupleException {
        //listTuples to not have concurrentModification
        this.filterNull(fieldToCompare).listTuples().stream().forEach((tuple) -> {
            this.removeRow(tuple);
        });
        return this;
    }

    public Table removeIfNotNull(String fieldToCompare) throws InvalidFieldException, KeyCollisionException, InvalidTupleException {
        //listTuples to not have concurrentModification
        this.filterNotNull(fieldToCompare).listTuples().stream().forEach((tuple) -> {
            this.removeRow(tuple);
        });
        return this;
    }

    public Table filterEqualFields(String field, String otherField) {

        Table answer = new Table(this.getSchema());
        Column fieldColumn = this.getColumn(field);
        Column otherFieldColumn = this.getColumn(otherField);
        if (fieldColumn == null || otherFieldColumn == null) {
            throw new InvalidFieldException();
        }
        if (fieldColumn.getType().equals(otherFieldColumn.getType())) {

            fieldColumn.getUniqueValues().stream().forEach((value) -> {
                this.filterEquals(field, value).filterEquals(otherField, value).stream().forEach((tuple) -> {
                    answer.referenceRow(tuple);
                });
            });

        }
        return answer;
    }

    public Table removeIfEqualFields(String field, String otherField) {
        this.filterEqualFields(field, otherField).stream().forEach((tuple) -> {
            this.removeRow(tuple);
        });
        return this;
    }

    public Stream<Tuple> stream() {
        return this.tuples.values().stream();
    }
}
