package simpledb.plan;

import simpledb.query.*;

public class AntijoinPlan implements Plan {
    private Plan p1, p2;
    private Predicate pred;

    public AntijoinPlan(Plan p1, Plan p2, Predicate pred) {
        this.p1 = p1;
        this.p2 = p2;
        this.pred = pred;
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new AntijoinScan(s1, s2, pred);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p1.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return p1.schema();
    }
}
