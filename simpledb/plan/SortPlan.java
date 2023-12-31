package simpledb.plan;

import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import java.util.List;
import java.util.ArrayList;

public class SortPlan implements Plan {
    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;
    private String sortfield; // Assuming a single sort field for simplicity
    private Index idx; // B-tree index

    public SortPlan(Plan p, String sortfield, Transaction tx) {
        this.p = p;
        this.tx = tx;
        this.sch = p.schema();
        this.sortfield = sortfield;
        this.comp = new RecordComparator(List.of(sortfield));

        // Create a B-tree index on the sort field
        this.idx = new BTreeIndex(tx, "tempindex", sch, sortfield);
    }

    @Override
    public Scan open() {
        Scan src = p.open();
        while (src.next()) {
            // Insert each record into the B-tree index
            idx.insert(src.getVal(sortfield), src.getRid());
        }
        src.close();

        // Open a B-tree traversal scan
        return new IndexScan(sch, idx, new TableScan(p, tx));
    }

    @Override
    public int blocksAccessed() {
        // Rough estimation of block accesses
        // Actual implementation would require detailed knowledge of the B-tree structure
        int height = idx.height();
        int leafNodes = idx.leafNodeCount();
        return height + leafNodes; // Height for traversing to leaf + leaf node accesses
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

    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next())
            return temps;

        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan))
            if (comp.compare(src, currentscan) < 0) {
                currentscan.close();
                currenttemp = new TempTable(tx, sch);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
            }
        currentscan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            List<TempTable> tomerge = new ArrayList<>();
            for (int i = 0; i < k && !runs.isEmpty(); i++)
                tomerge.add(runs.remove(0));
            result.add(mergeRuns(tomerge));
        }
        if (!runs.isEmpty())
            result.add(runs.get(0));
        return result;
    }

    private TempTable mergeRuns(List<TempTable> tomerge) {
        List<Scan> scns = new ArrayList<>();
        for (TempTable tt : tomerge)
            scns.add(tt.open());

        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();
        PriorityQueue<Record> pq = new PriorityQueue<>(comp);
        for (Scan s : scns) {
            if (s.next()) {
                pq.add(new Record(s, s.getRid()));
            }
        }

        try {
            while (!pq.isEmpty()) {
                Record smallest = pq.poll();
                copy(smallest.s, dest);
                if (smallest.s.next())
                    pq.add(new Record(smallest.s, smallest.s.getRid()));
            }
        } finally {
            for (Scan s : scns)
                s.close();
            dest.close();
        }
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public List<String> getSortedFields() {
        return sortfields;
    }


    private class Record implements Comparable<Record> {
        Scan s;
        RID rid;

        Record(Scan s, RID rid) {
            this.s = s;
            this.rid = rid;
        }

        @Override
        public int compareTo(Record r) {
            return comp.compare(s, r.s);
        }
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public List<String> getSortedFields() {
        return List.of(sortfield);
    }
}
