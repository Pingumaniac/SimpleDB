package simpledb.plan;

import simpledb.query.*;
import java.util.*;

public class RenamePlan implements Plan {
    private Plan p;
    private Map<String, String> fieldMapping;

    public RenamePlan(Plan p, Map<String, String> fieldMapping) {
        this.p = p;
        this.fieldMapping = new HashMap<>(fieldMapping);
    }

    public Scan open() {
        Scan s = p.open();
        return new RenameScan(s, fieldMapping);
    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String fldname) {
        String originalFldname = fieldMapping.getOrDefault(fldname, fldname);
        return p.distinctValues(originalFldname);
    }

    public Schema schema() {
        Schema originalSchema = p.schema();
        Schema renamedSchema = new Schema();
        for (String fldname : originalSchema.fields()) {
            String newFldname = fieldMapping.getOrDefault(fldname, fldname);
            renamedSchema.addField(newFldname, originalSchema.type(fldname), originalSchema.length(fldname));
        }
        return renamedSchema;
    }
}
