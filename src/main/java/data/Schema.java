package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class Schema implements Serializable {
    private List<String> fields;
    private Map<String, Class> fieldsTypes;
    private Map<String, List<String>> keys; //name of the key and name of the fields
    private Boolean lock;
    private Map<String, Collection<String>> keyedColumns; //<name of the column, name of the key>
    private Map<String, Object> defaultValues;

    public Schema() {
        this.init();
    }

    public Schema(Schema cloned) {
        this.fields = new ArrayList<>(cloned.fields);
        this.fieldsTypes = new HashMap<>(cloned.fieldsTypes);
        this.keys = new HashMap<>(cloned.keys);
        this.lock = cloned.lock;
        this.keyedColumns = new HashMap<>(cloned.keyedColumns);
        this.defaultValues = new HashMap<>(cloned.defaultValues);
    }

    public Schema(final Collection<String> fieldNames, final Collection<Class> types) {

        this.init();
        if (fieldNames.size() != types.size()) {
            throw new DataException();
        } else {
            String s;
            Iterator<String> nit = fieldNames.iterator();
            Iterator<Class> cit = types.iterator();
            while (nit.hasNext()) {
                s = nit.next();
                if (!fieldsTypes.containsKey(s)) {
                    fields.add(s);
                    fieldsTypes.put(s, cit.next());
                } else {
                    throw new DataException();
                }
            }
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.fields);
        hash = 97 * hash + Objects.hashCode(this.fieldsTypes);
        hash = 97 * hash + Objects.hashCode(this.keys);
        hash = 97 * hash + Objects.hashCode(this.keyedColumns);
        hash = 97 * hash + Objects.hashCode(this.defaultValues);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Schema other = (Schema) obj;
        if (!Objects.equals(this.fields, other.fields)) {
            return false;
        }
        if (!Objects.equals(this.fieldsTypes, other.fieldsTypes)) {
            return false;
        }
        if (!Objects.equals(this.keys, other.keys)) {
            return false;
        }
        if (!Objects.equals(this.keyedColumns, other.keyedColumns)) {
            return false;
        }
        return Objects.equals(this.defaultValues, other.defaultValues);
    }
    
    

    public String getField(int index) {
        if(index<0 || index>=fields.size()){
            throw new InvalidFieldException(index+"");
        }
        return fields.get(index);
    }
    
    public Integer getIndexOf(String field){
        Integer answer = fields.indexOf(field);
        if(answer<0){
            throw new InvalidFieldException(field);
        }
        return answer;
    }

    private void init() {
        this.fields = new ArrayList<>();
        this.fieldsTypes = new HashMap<>();
        this.keys = new HashMap<>();
        this.lock = false;
        this.keyedColumns = new HashMap<>();
        this.defaultValues = new HashMap<>();
    }

    public int getSize() {
        return this.fields.size();
    }

    public Schema addField(String name, final Class type) {
        if (!lock) {
            if(!this.fieldsTypes.containsKey(name)){
                this.fields.add(name);
                this.fieldsTypes.put(name, type);
            }
            else{
                throw new DataException();
            }
        }
        return this;
    }

    public Schema addField(String name, final Class type, Object defValue) {
        if (!lock) {
            if(!this.fieldsTypes.containsKey(name)){
                this.fields.add(name);
                this.fieldsTypes.put(name, type);
                this.defaultValues.put(name, defValue);
            }
            else{
                throw new DataException();
            }
        }
        return this;
    }

    public Schema setDefaultValue(String field, Object value) {
        if (!lock) {
            if (this.fieldsTypes.containsKey(field) && fieldsTypes.get(field).isAssignableFrom(value.getClass())) {
                this.defaultValues.put(field, value);
            } else {
                throw new DataException();
            }
        }
        return this;
    }

    public Schema removeField(String name) {

        if (!(lock || keyedColumns.containsKey(name))) {
            this.fields.remove(name);
        }
        return this;
    }

    public Schema setKeyByColumns(String keyName, final List<String> fieldsNames) {

        if (!fieldsTypes.keySet().containsAll(fieldsNames)) {
            throw new DataException();
        }
        keys.put(keyName, fieldsNames);
        for (String s : fieldsNames) {
            //Collection<String> keys;
            if (!keyedColumns.containsKey(s)) {
                keyedColumns.put(s, new ArrayList<String>());
            }
            keyedColumns.get(s).add(keyName);
        }
        return this;
    }

    public Schema setKeyByColumns(String keyName, String... fieldsNames) {

        List<String> names = Arrays.asList(fieldsNames);
        setKeyByColumns(keyName, names);
        return this;
    }
    
    public Schema removeKey(String keyName){
        if(keyedColumns.containsKey(keyName)){
            keyedColumns.remove(keyName);
        }
        return this;
    }

    public Schema lockSchema() {
        this.lock = true;
        return this;
    }

    public Schema unlockSchema() {
        this.lock = false;
        return this;
    }

    @Deprecated
    public Iterator<String> getKeyFields(String keyName) {
        if(keys.containsKey(keyName)){
            return keys.get(keyName).iterator();
        }
        else{
            return new ArrayList<String>(0).iterator();
        }
    }
    
    public Stream<String> getKeyFieldsStream(String keyName){
        if(keys.containsKey(keyName)){
            return keys.get(keyName).stream();
        }
        else{
            return new ArrayList<String>(0).stream();
        }
    }
    
    public Stream<String> getKeyFieldsParallelStream(String keyName){
        if(keys.containsKey(keyName)){
            return keys.get(keyName).parallelStream();
        }
        else{
            return new ArrayList<String>(0).stream();
        }
    }
    
    public Set<String> getKeyFieldSet(String keyName){
    	Set<String> answer =  new HashSet<>();
        if(keys.containsKey(keyName)){
            answer.addAll(keys.get(keyName));
        }
    	return answer;
    }

    public String getKeyField(String keyName, int index) throws InvalidKeyException{
        if(keys.containsKey(keyName)){
            return keys.get(keyName).get(index);
        }
        else{
            throw new InvalidKeyException(keyName);
        }
    }
    
    @Deprecated
    public Iterator<String> getFields() {
        return fields.iterator();
    }
    
    public Stream<String> getFieldsStream(){
        return fields.stream();
    }
    
    public Stream<String> getFieldsParallelStream(){
        return fields.parallelStream();
    }
        
    @Deprecated
    public Set<String> getFieldSet(){
    	Set<String> answer =  new HashSet<>();
    	answer.addAll(fields);
    	return answer;
    }

    public Class getFieldType(String field) {
        return fieldsTypes.get(field);
    }

    public boolean hasField(String field) {
        return fieldsTypes.containsKey(field);
    }

    @Deprecated
    public Iterator<String> getKeys() {
        return keys.keySet().iterator();
    }
    
    public Stream<String > getKeysNamesStream(){
        return keys.keySet().stream();
    }
    
    public Stream<String > getKeysNamesParallelStream(){
        return keys.keySet().parallelStream();
    }
        
    public Stream<Pair<String, List<String>> > getKeysStream(){
        
        Function< Map.Entry<String, List<String>>, Pair<String, List<String>> > function = (entry) -> {
            return new Pair<>(entry.getKey(), entry.getValue());
        };
        
        return this.keys.entrySet().stream().map(function);
    }

    public Stream<Pair<String, List<String>> > getKeysParallelStream(){
        
        Function< Map.Entry<String, List<String>>, Pair<String, List<String>> > funtion = (entry) -> {
            return new Pair<>(entry.getKey(), entry.getValue());
        };
        
        return this.keys.entrySet().parallelStream().map(funtion);
    }    
    
    public boolean isLocked() {
        return this.lock;
    }

    public boolean hasKey() {
        return keys.size() > 0;
    }

    @Override
    public String toString() {
    	String answer = "";
    	for(String key : this.keys.keySet()){
    		answer +=  "key: "+ key + " "+this.keys.get(key)+"\n";
    	}
    	answer += "[";
    	for(String field : fieldsTypes.keySet()){
    		answer += fieldsTypes.get(field).getSimpleName() + "::" + field + ", ";
    	}
    	answer += "]";
        return answer;
    }

    public boolean isPartOfAKey(String fieldName) {
        return keyedColumns.containsKey(fieldName);
    }

    @Deprecated
    public Iterator<String> isPartOfKeys(String fieldName) {
        if (keyedColumns.containsKey(fieldName)) {
            return keyedColumns.get(fieldName).iterator();
        }

        return new ArrayList<String>().iterator();
    }
    
    public Stream<String> isPartOfKeysStream(String fieldName) {
        if (keyedColumns.containsKey(fieldName)) {
            return keyedColumns.get(fieldName).stream();
        }

        return new ArrayList<String>().stream();
    }
        
    public Stream<String> isPartOfKeysParallelStream(String fieldName) {
        if (keyedColumns.containsKey(fieldName)) {
            return keyedColumns.get(fieldName).parallelStream();
        }

        return new ArrayList<String>().parallelStream();
    }

    public int getKeySize(String keyName) {
        if (keys.containsKey(keyName)) {
            return keys.get(keyName).size();
        } else {
            return 0;
        }
    }

    public Object getDefaultValue(String field) {
        if (this.defaultValues.containsKey(field)) {
            return this.defaultValues.get(field);
        } else {
            return null;
        }
    }
    
    public Object getDefaultValue(int index) {
        String field = this.getField(index);
        if (this.defaultValues.containsKey(field)) {
            return this.defaultValues.get(field);
        } else {
            return null;
        }
    }
}
