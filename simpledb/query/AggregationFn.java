package simpledb.query;

public interface AggregationFn {
    void processFirst(Scan s);
    void processNext(Scan s);
    String fieldName();
    Constant value();
}
