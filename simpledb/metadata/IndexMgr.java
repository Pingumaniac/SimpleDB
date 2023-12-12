package simpledb.metadata;

import java.util.*;
import static simpledb.metadata.TableMgr.MAX_NAME;
import simpledb.tx.Transaction;
import simpledb.record.TableScan;
import simpledb.record.*;

class IndexMgr {
   private Layout layout;
   private TableMgr tblmgr;
   private StatMgr statmgr;

   public IndexMgr(boolean isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
      if (isnew) {
         Schema sch = new Schema();
         sch.addStringField("indexname", MAX_NAME);
         sch.addStringField("tablename", MAX_NAME);
         sch.addStringField("fieldname", MAX_NAME);
         sch.addStringField("indextype", MAX_NAME); // New field for index type
         tblmgr.createTable("idxcat", sch, tx);
      }
      this.tblmgr = tblmgr;
      this.statmgr = statmgr;
      layout = tblmgr.getLayout("idxcat", tx);
   }
   
   public void createIndex(String idxname, String tblname, String fldname, String indextype, Transaction tx) {
      TableScan fcat = new TableScan(tx, "fldcat", tblmgr.getLayout("fldcat", tx));
      while (fcat.next()) {
         if (fcat.getString("tblname").equals(tblname) && fcat.getString("fldname").equals(fldname)) {
            fcat.setString("indexname", idxname);
            fcat.setString("indextype", indextype);
            break;
         }
      }
      fcat.close();
   }

   public Map<String,IndexInfo> getIndexInfo(String tblname, Transaction tx) {
      Map<String,IndexInfo> result = new HashMap<String,IndexInfo>();
      TableScan ts = new TableScan(tx, "idxcat", layout);
      while (ts.next())
         if (ts.getString("tablename").equals(tblname)) {
         String idxname = ts.getString("indexname");
         String fldname = ts.getString("fieldname");
         Layout tblLayout = tblmgr.getLayout(tblname, tx);
         StatInfo tblsi = statmgr.getStatInfo(tblname, tblLayout, tx);
         IndexInfo ii = new IndexInfo(idxname, fldname, tblLayout.schema(), tx, tblsi);
         result.put(fldname, ii);
      }
      ts.close();
      return result;
   }
}
