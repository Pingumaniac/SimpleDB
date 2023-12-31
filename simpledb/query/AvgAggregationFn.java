package simpledb.query;

public class AvgAggregationFn implements AggregationFn {
    private String fldname;
    private int count;
    private double sum;

    public AvgAggregationFn(String fldname) {
        this.fldname = fldname;
    }

    @Override
    public void processFirst(Scan s) {
        sum = s.getDouble(fldname);
        count = 1;
    }

    @Override
    public void processNext(Scan s) {
        sum += s.getDouble(fldname);
        count++;
    }

    @Override
    public String fieldName() {
        return "avgof" + fldname;
    }

    @Override
    public Constant value() {
        double avg = count > 0 ? (sum / count) : 0;
        return new Constant(avg);
    }
}
