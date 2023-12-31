package simpledb.plan;

import simpledb.query.*;
import simpledb.tx.Transaction;

public class NoDupsPlan implements Plan {
    private Plan p;

    public NoDupsPlan(Plan p) {
        this.p = p;
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new NoDupsScan(s, p.schema());
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed() + NoDupsScan.estimatedAdditionalBlocksAccessed(p.recordsOutput());
    }

    @Override
    public int recordsOutput() {
        // Distinct values are less than or equal to total records
        return Math.min(p.recordsOutput(), p.distinctValues(p.schema().fields().get(0)));
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname); // Distinct values remain the same per field
    }

    @Override
    public Schema schema() {
        return p.schema();
    }
}
