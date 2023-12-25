package simpledb.plan;

import simpledb.query.*;
import java.util.*;

public class ExtendPlan implements Plan {
    private Plan p;
    private Map<String, Expression> newFields;

    public ExtendPlan(Plan p, Map<String, Expression> newFields) {
        this.p = p;
        this.newFields = new HashMap<>(newFields);
    }

    public Scan open() {
        Scan s = p.open();
        return new ExtendScan(s, newFields);
    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String fldname) {
        if (newFields.containsKey(fldname)) {
            return 1;
        }
        return p.distinctValues(fldname);
    }

    public Schema schema() {
        Schema newSchema = new Schema();
        newSchema.addAll(p.schema());
        for (Map.Entry<String, Expression> entry : newFields.entrySet()) {
            newSchema.addField(entry.getKey(), entry.getValue().getType(), entry.getValue().length());
        }
        return newSchema;
    }
}
