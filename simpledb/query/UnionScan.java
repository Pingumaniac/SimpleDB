package simpledb.query;

import java.util.NoSuchElementException;

public class UnionScan implements Scan {
    private Scan s1, s2;
    private boolean inFirstScan;

    public UnionScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
        this.inFirstScan = true;
        s1.beforeFirst();
        if (!s1.next()) {
            inFirstScan = false;
            s2.beforeFirst();
        }
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
        inFirstScan = true;
        if (!s1.next()) {
            inFirstScan = false;
        }
    }

    @Override
    public boolean next() {
        if (inFirstScan) {
            if (s1.next()) {
                return true;
            } else {
                inFirstScan = false;
                s2.beforeFirst();
            }
        }
        return s2.next();
    }

    @Override
    public int getInt(String fldname) {
        if (inFirstScan) {
            return s1.getInt(fldname);
        } else {
            return s2.getInt(fldname);
        }
    }

    @Override
    public String getString(String fldname) {
        if (inFirstScan) {
            return s1.getString(fldname);
        } else {
            return s2.getString(fldname);
        }
    }

    @Override
    public Constant getVal(String fldname) {
        if (inFirstScan) {
            return s1.getVal(fldname);
        } else {
            return s2.getVal(fldname);
        }
    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}