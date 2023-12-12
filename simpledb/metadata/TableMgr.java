package simpledb.metadata;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;

class TableMgr {
   // The max characters a tablename or fieldname can have.
   public static final int MAX_NAME = 16;
   private Layout tcatLayout, fcatLayout;
   private int uniqueIdCounter; // Counter for unique ID generation

   public TableMgr(boolean isNew, Transaction tx) {
      uniqueIdCounter = 0;
      Schema tcatSchema = new Schema();
      tcatSchema.addIntField("tblId"); // New artificial key
      tcatSchema.addStringField("tblname", MAX_NAME);
      tcatSchema.addIntField("slotsize");
      tcatLayout = new Layout(tcatSchema);

      Schema fcatSchema = new Schema();
      fcatSchema.addIntField("tableId"); // New foreign key referencing tblId
      fcatSchema.addStringField("fldname", MAX_NAME);
      fcatSchema.addIntField("type");
      fcatSchema.addIntField("length");
      fcatSchema.addIntField("offset");
      fcatLayout = new Layout(fcatSchema);

      if (isNew) {
         createTable("tblcat", tcatSchema, tx);
         createTable("fldcat", fcatSchema, tx);
      }
   }

   public void createTable(String tblname, Schema sch, Transaction tx) {
      tx.start(); // Start the transaction
      try {
         Layout layout = new Layout(sch);
         // Insert one record into tblcat
         TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
         tcat.insert();
         tcat.setInt("tblId", generateUniqueId()); // Generate a unique ID for the table
         tcat.setString("tblname", tblname);
         tcat.setInt("slotsize", layout.slotSize());
         tcat.close();
         // Insert a record into fldcat for each field
         TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
         int tableId = generateUniqueId(); // Generate a unique ID for the foreign key reference
         for (String fldname : sch.fields()) {
            fcat.insert();
            fcat.setInt("tableId", tableId); // Set the foreign key
            fcat.setString("fldname", fldname);
            fcat.setInt("type", sch.type(fldname));
            fcat.setInt("length", sch.length(fldname));
            fcat.setInt("offset", layout.offset(fldname));
         }
         fcat.close();
         tx.commit(); // Commit the transaction
      } catch (Exception e) {
         tx.rollback(); // Rollback in case of an exception
         throw e;
      }
   }

   private int generateUniqueId() {
      return ++uniqueIdCounter; // Increment and return the counter value
   }

   public void dropTable(String tblname, Transaction tx) {
      // Remove table from tblcat
      TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
      while (tcat.next())
         if (tcat.getString("tblname").equals(tblname)) {
            tcat.delete();
            break;
         }
      tcat.close();

      // Remove fields from fldcat
      TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
      while (fcat.next())
         if (fcat.getString("tblname").equals(tblname)) {
            fcat.delete();
         }
      fcat.close();
   }



   public Layout getLayout(String tblname, Transaction tx) {
      int size = -1;
      TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
      while (tcat.next())
         if (tcat.getString("tblname").equals(tblname)) {
            size = tcat.getInt("slotsize");
            break;
         }
      tcat.close();

      if (size == -1)
         return null;

      Schema sch = new Schema();
      Map<String, Integer> offsets = new HashMap<String, Integer>();
      TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
      while (fcat.next())
         if (fcat.getString("tblname").equals(tblname)) {
            String fldname = fcat.getString("fldname");
            int fldtype = fcat.getInt("type");
            int fldlen = fcat.getInt("length");
            int offset = fcat.getInt("offset");
            offsets.put(fldname, offset);
            sch.addField(fldname, fldtype, fldlen);
         }
      fcat.close();
      return new Layout(sch, offsets, size);
   }

   public void removeField(String tblname, String fldname, Transaction tx) {
      TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
      while (fcat.next()) {
         if (fcat.getString("tblname").equals(tblname) &&
                 fcat.getString("fldname").equals(fldname)) {
            fcat.setString("fldname", "", true);
            break;
         }
      }
      fcat.close();
   }

   public void printTableNamesAndFields(Transaction tx) {
      TableScan tblcat = new TableScan(tx, "tblcat", getLayout("tblcat", tx));
      while (tblcat.next()) {
         String tblname = tblcat.getString("tblname");
         System.out.print(tblname + "(");

         TableScan fldcat = new TableScan(tx, "fldcat", getLayout("fldcat", tx));
         while (fldcat.next()) {
            if (fldcat.getString("tblname").equals(tblname)) {
               String fldname = fldcat.getString("fldname");
               System.out.print(fldname + ", ");
            }
         }
         fldcat.close();
         System.out.println(")");
      }
      tblcat.close();
   }

   public void printCreateTableStatement(String tableName, Transaction tx) {
      TableScan tblcat = new TableScan(tx, "tblcat", getLayout("tblcat", tx));
      boolean tableExists = false;

      while (tblcat.next()) {
         if (tblcat.getString("tblname").equals(tableName)) {
            tableExists = true;
            break;
         }
      }
      tblcat.close();

      if (!tableExists) {
         System.out.println("Table does not exist.");
         return;
      }

      StringBuilder createStmt = new StringBuilder("create table ");
      createStmt.append(tableName).append(" (");

      TableScan fldcat = new TableScan(tx, "fldcat", getLayout("fldcat", tx));
      while (fldcat.next()) {
         if (fldcat.getString("tblname").equals(tableName)) {
            String fldname = fldcat.getString("fldname");
            int fldtype = fldcat.getInt("type");
            int fldlen = fldcat.getInt("length");
            createStmt.append(fldname).append(" ")
                    .append(fldtype == INTEGER ? "integer" : "varchar(" + fldlen + ")").append(", ");
         }
      }
      fldcat.close();
      createStmt.delete(createStmt.length() - 2, createStmt.length()); // Remove last comma and space
      createStmt.append(")");
      System.out.println(createStmt);
   }
}