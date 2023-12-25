package simpledb.query;

public class ExtendScan implements Scan {
    private Scan s;
    private Expression expr;
    private String newFieldName;

    public ExtendScan(Scan s, Expression expr, String newFieldName) {
        this.s = s;
        this.expr = expr;
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
        if (fldname.equals(newFieldName)) {
            return expr.evaluate(s).asInt();
        }
        return s.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (fldname.equals(newFieldName)) {
            return expr.evaluate(s).asString();
        }
        return s.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (fldname.equals(newFieldName)) {
            return expr.evaluate(s);
        }
        return s.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        if (fldname.equals(newFieldName)) {
            return true;
        }
        return s.hasField(fldname);
    }

    @Override
    public void close() {
        s.close();
    }
}
