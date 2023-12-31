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
    private int k; // for k-way merge

    public SortPlan(Plan p, List<String> sortfields, Transaction tx, int k) {
        this.p = p;
        this.tx = tx;
        this.sch = p.schema();
        this.comp = new RecordComparator(sortfields);
        this.k = k;
    }

    @Override
    public Scan open() {
        Scan src = p.open();
        if (!src.next()) {
            src.close();
            return new EmptyScan(); // Handle empty table case
        }
        src.beforeFirst();

        List<TempTable> runs = splitIntoRuns(src);
        src.close();

        while (runs.size() > 1)
            runs = doAMergeIteration(runs);

        return new SortScan(runs, comp);
    }

    @Override
    public int blocksAccessed() {
        // This does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(tx, p);
        return mp.blocksAccessed();
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
}
