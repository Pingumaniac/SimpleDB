package simpledb.plan;

import simpledb.query.*;
import simpledb.tx.Transaction;
import java.util.*;

public class GBNoDupsPlan implements Plan {
    private Plan p;
    private List<String> fields;

    public GBNoDupsPlan(Plan p, List<String> fields) {
        this.p = p;
        this.fields = fields;
    }

    @Override
    public Scan open() {
        List<AggregationFn> emptyAggList = new ArrayList<>(); // No aggregation functions
        return new GroupByPlan(p, fields, emptyAggList, p.schema()).open();
    }

    @Override
    public int blocksAccessed() {
        int groupByBlocks = p.blocksAccessed() * 2;
        int dedupBlocks = NoDupsScan.estimatedAdditionalBlocksAccessed(p.recordsOutput());
        return groupByBlocks + dedupBlocks;
    }

    @Override
    public int recordsOutput() {
        return Math.min(p.recordsOutput(), p.distinctValues(p.schema().fields().get(0)));
    }

    @Override
    public int distinctValues(String fldname) {
        // In group by, distinct values in fields are equal to the number of groups
        return fields.contains(fldname) ? recordsOutput() : p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return p.schema();
    }
}
