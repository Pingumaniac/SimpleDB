package simpledb.query;

public class SumAggregationFn implements AggregationFn {
    private String fldname;
    private double sum;

    public SumAggregationFn(String fldname) {
        this.fldname = fldname;
    }

    @Override
    public void processFirst(Scan s) {
        sum = s.getDouble(fldname);
    }

    @Override
    public void processNext(Scan s) {
        sum += s.getDouble(fldname);
    }

    @Override
    public String fieldName() {
        return "sumof" + fldname;
    }

    @Override
    public Constant value() {
        return new Constant(sum);
    }
}
