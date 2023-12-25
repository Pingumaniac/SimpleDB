package simpledb.query;

public class SemijoinScan implements Scan {
    private Scan s1, s2;
    private Predicate pred;
    private boolean hasMore;

    public SemijoinScan(Scan s1, Scan s2, Predicate pred) {
        this.s1 = s1;
        this.s2 = s2;
        this.pred = pred;
        this.hasMore = true;
        s1.beforeFirst();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        hasMore = true;
    }

    @Override
    public boolean next() {
        while (hasMore && s1.next()) {
            s2.beforeFirst();
            while (s2.next()) {
                if (pred.isSatisfied(s1, s2)) {
                    return true;
                }
            }
        }
        hasMore = false;
        return false;
    }

    @Override
    public int getInt(String fldname) {
        return s1.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return s1.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return s1.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}