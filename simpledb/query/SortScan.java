package simpledb.query;

import simpledb.record.RID;
import java.util.*;

public class SortScan implements Scan {
    private List<Scan> runs;
    private RecordComparator comp;
    private PriorityQueue<Record> heap;

    public SortScan(List<TempTable> runs, RecordComparator comp) {
        this.comp = comp;
        this.runs = new ArrayList<>();
        for (TempTable run : runs)
            this.runs.add(run.open());
        heap = new PriorityQueue<>(comp);
        loadHeap();
    }

    @Override
    public void beforeFirst() {
        close();
        heap.clear();
        for (Scan run : runs)
            run.beforeFirst();
        loadHeap();
    }

    @Override
    public boolean next() {
        if (heap.isEmpty()) return false;
        Record smallest = heap.poll();
        if (smallest.s.next())
            heap.add(new Record(smallest.s, smallest.s.getRid()));
        return true;
    }

    @Override
    public void close() {
        for (Scan s : runs)
            s.close();
    }

    private void loadHeap() {
        for (Scan s : runs) {
            if (s.next()) {
                heap.add(new Record(s, s.getRid()));
            }
        }
    }

    @Override
    public Constant getVal(String fldname) {
        return heap.peek().s.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return heap.peek().s.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return heap.peek().s.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return heap.peek().s.hasField(fldname);
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
