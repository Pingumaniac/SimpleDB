package simpledb.query;

public class RenameScan implements Scan {
    private Scan s;
    private String oldFieldName;
    private String newFieldName;

    public RenameScan(Scan s, String oldFieldName, String newFieldName) {
        this.s = s;
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public int getInt(String fldname) {
        fldname = renameField(fldname);
        return s.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        fldname = renameField(fldname);
        return s.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        fldname = renameField(fldname);
        return s.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        fldname = renameField(fldname);
        return s.hasField(fldname);
    }

    @Override
    public void close() {
        s.close();
    }

    private String renameField(String fldname) {
        if (fldname.equals(newFieldName)) {
            return oldFieldName;
        }
        return fldname;
    }
}