package simpledb.query;

import java.util.*;

public class Record {
    private Map<String, Constant> values;

    public Record(Scan s) {
        values = new HashMap<>();
        for (String fldname : s.getSchema().fields()) {
            values.put(fldname, s.getVal(fldname));
        }
    }

    public Constant getVal(String fldname) {
        return values.get(fldname);
    }
}
