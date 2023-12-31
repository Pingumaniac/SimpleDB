package simpledb.plan;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.parse.*;
import simpledb.query.*;

public class BasicQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            String viewdef = mdm.getViewDef(tblname, tx);
            if (viewdef != null) {
                Parser parser = new Parser(viewdef);
                QueryData viewdata = parser.query();
                plans.add(createPlan(viewdata, tx));
            } else {
                plans.add(new TablePlan(tx, tblname, mdm));
            }
        }

        // Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans)
            p = new ProductPlan(p, nextplan);

        // Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        // Step 4: Add a plan for GROUP BY clause if present
        if (data.hasGroupBy()) {
            p = new GroupByPlan(p, data.groupFields(), data.fields(), tx);
        }

        // Step 5: Project on the field names
        p = new ProjectPlan(p, data.fields());

        // Step 6: If DISTINCT is specified, add a plan to remove duplicates
        if (data.isDistinct()) {
            p = new NoDuplicatesPlan(p);
        }

        // Step 7: Handle ORDER BY clause
        if (data.hasOrderBy()) {
            p = new SortPlan(p, data.orderFields(), tx);
        }

        return p;
    }
}
