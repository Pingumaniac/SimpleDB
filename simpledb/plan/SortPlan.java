package simpledb.plan;

import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import java.util.*;

public class SortPlan implements Plan {
    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;
    private String sortfield;
    private int runSize; // Size of each run in blocks

    public SortPlan(Plan p, String sortfield, Transaction tx, int runSize) {
        this.p = p;
        this.tx = tx;
        this.sch = p.schema();
        this.sortfield = sortfield;
        this.comp = new RecordComparator(List.of(sortfield));
        this.runSize = runSize; // Initialize run size
    }

    @Override
    public Scan open() {
        Scan src = p.open();
        if (!src.next()) {
            src.close();
            return new EmptyScan();
        }
        src.beforeFirst();
        List<TempTable> runs = splitIntoRuns(src, runSize);
        src.close();
        int k = calculateK(runs.size());

        while (runs.size() > 1)
            runs = doAMergeIteration(runs, k);

        return new SortScan(runs, comp);
    }

    private List<TempTable> splitIntoRuns(Scan src, int runSize) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        while (src.next()) {
            TempTable currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            UpdateScan currentscan = currenttemp.open();

            int blockSize = 0;
            do {
                copy(src, currentscan);
                blockSize++;
            } while (blockSize < runSize && src.next());

            currentscan.close();
        }
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs, int k) {
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

    private int calculateK(int numRuns) {
        // Assuming k is limited by the number of available buffers
        int availableBuffers = tx.availableBuffers();
        return Math.min(numRuns, availableBuffers);
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
    public boolean isSorted() {
        return true;
    }

    @Override
    public List<String> getSortedFields() {
        return sortfields;
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
