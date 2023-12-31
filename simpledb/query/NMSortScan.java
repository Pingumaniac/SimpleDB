package simpledb.query;

import simpledb.record.RID;
import java.util.*;

public class NMSortScan implements Scan {
    private Scan s;
    private List<String> sortfields;
    private List<RID> sortedRids;
    private Iterator<RID> iter;

    public NMSortScan(Scan s, List<String> sortfields) {
        this.s = s;
        this.sortfields = sortfields;
        this.sortedRids = new ArrayList<>();
        sortRids();
        iter = sortedRids.iterator();
    }

    private void sortRids() {
        // Collect all RIDs
        s.beforeFirst();
        while (s.next()) {
            sortedRids.add(s.getRid());
        }

        // Sort the RIDs based on the sortfields
        sortedRids.sort((rid1, rid2) -> {
            s.moveToRid(rid1);
            List<Constant> values1 = getSortFieldValues();
            s.moveToRid(rid2);
            List<Constant> values2 = getSortFieldValues();

            for (int i = 0; i < sortfields.size(); i++) {
                int comp = values1.get(i).compareTo(values2.get(i));
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        });
    }

    private List<Constant> getSortFieldValues() {
        List<Constant> values = new ArrayList<>();
        for (String fldname : sortfields) {
            values.add(s.getVal(fldname));
        }
        return values;
    }

    @Override
    public void beforeFirst() {
        iter = sortedRids.iterator();
    }

    @Override
    public boolean next() {
        if (!iter.hasNext()) return false;
        RID rid = iter.next();
        s.moveToRid(rid);
        return true;
    }

    // Delegate other Scan methods to 's'
    @Override
    public int getInt(String fldname) {
        return s.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return s.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return s.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    @Override
    public void close() {
        s.close();
    }

    public RID getRid() {
        return s.getRid();
    }

    public void moveToRid(RID rid) {
        s.moveToRid(rid);
    }
}