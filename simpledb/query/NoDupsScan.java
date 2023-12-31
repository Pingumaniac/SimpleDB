package simpledb.query;

import java.util.HashSet;
import java.util.Set;

public class NoDupsScan implements Scan {
    private Scan s;
    private Set<Constant> seen;
    private Schema schema;

    public NoDupsScan(Scan s, Schema schema) {
        this.s = s;
        this.schema = schema;
        seen = new HashSet<>();
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
        seen.clear();
    }

    @Override
    public boolean next() {
        while (s.next()) {
            Constant key = getDedupKey();
            if (!seen.contains(key)) {
                seen.add(key);
                return true;
            }
        }
        return false;
    }

    private Constant getDedupKey() {
        // Create a unique key for each record based on all fields
        StringBuilder keyBuilder = new StringBuilder();
        for (String field : schema.fields()) {
            keyBuilder.append(s.getVal(field).hashCode()); // Use hash code for uniqueness
            keyBuilder.append("|"); // Delimiter to separate field values
        }
        return new Constant(keyBuilder.toString());
    }

    public Constant getVal(String fldname) {
        return s.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return s.getInt(fldname);
    }

    @Override
    public String getString(String fldname)  {
        return s.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    @Override
    public void close() {
        s.close();
    }
}
