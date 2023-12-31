package simpledb.query;

import java.util.*;

public class HashJoinScan implements Scan {
    private Scan s1, s2;
    private String fldname1, fldname2;
    private Map<Constant, Record> hashTable;
    private Record currentRecord;

    public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.hashTable = new HashMap<>();

        // Load the hash table with records from s1
        while (s1.next()) {
            Constant key = s1.getVal(fldname1);
            hashTable.put(key, new Record(s1));
        }
    }

    @Override
    public void beforeFirst() {
        s2.beforeFirst();
        currentRecord = null;
    }

    @Override
    public boolean next() {
        while (currentRecord == null && s2.next()) {
            Constant key = s2.getVal(fldname2);
            if (hashTable.containsKey(key)) {
                currentRecord = hashTable.get(key);
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
            return currentRecord.getVal(fldname);
        else
            return s2.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return getVal(fldname).asInt();
    }

    @Override
    public String getString(String fldname) {
        return getVal(fldname).asString();
    }

    @Override
    public boolean hasField(String fldname) {
        return sch.hasField(fldname);
    }
}
