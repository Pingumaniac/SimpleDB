package simpledb.plan;

import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import java.util.List;

public class NMSortPlan implements Plan {
    private Plan p;
    private Transaction tx;
    private Schema sch;
    private List<String> sortfields;

    public NMSortPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.sortfields = sortfields;
        this.tx = tx;
        this.sch = p.schema();
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new NMSortScan(s, sortfields);
    }

    @Override
    public int blocksAccessed() {
        // Estimate based on the underlying plan
        return p.blocksAccessed(); // This is a simplistic estimate
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return sch;
    }
}