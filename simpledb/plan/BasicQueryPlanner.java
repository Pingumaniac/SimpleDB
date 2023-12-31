package simpledb.plan;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.parse.*;

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
                // Process view
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

        // Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());

        // Step 5: Handle ORDER BY clause
        if (data.hasOrderBy()) {
            p = new SortPlan(p, data.orderFields(), tx);
        }

        return p;
    }
}
