package simpledb.plan;

import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Transaction tx;
    private Schema sch = new Schema();

    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.tx = tx;
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.sch.addAll(p1.schema());
        this.sch.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new HashJoinScan(s1, s2, fldname1, fldname2);
    }

    @Override
    public int blocksAccessed() {
        int lhsBlocks = p1.blocksAccessed();
        int rhsBlocks = p2.blocksAccessed();
        return lhsBlocks + rhsBlocks; // Sum of blocks accessed by both plans
    }

    @Override
    public int recordsOutput() {
        // Estimate based on the size of the output records
        int lhsRecords = p1.recordsOutput();
        int rhsRecords = p2.recordsOutput();
        return (int) (lhsRecords * (rhsRecords / (double) p2.distinctValues(fldname2)));
    }

    @Override
    public int distinctValues(String fldname) {
        // Combine distinct values from both sides
        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname);
        else
            return p2.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
