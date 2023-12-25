package simpledb.plan;

import simpledb.tx.Transaction;
import simpledb.parse.QueryData;

public interface QueryPlanner {

   public Plan createPlan(QueryData data, Transaction tx);
}
