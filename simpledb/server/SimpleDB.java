package simpledb.server;

import java.io.File;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.buffer.BufferMgr;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.*;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.opt.HeuristicQueryPlanner;

public class SimpleDB {
   public static int BLOCK_SIZE = 400;
   public static int BUFFER_SIZE = 8;
   public static String LOG_FILE = "simpledb.log";

   private  FileMgr     fm;
   private  BufferMgr   bm;
   private  LogMgr      lm;
   private  MetadataMgr mdm;
   private  Planner planner;

   public SimpleDB(String dirname, int blocksize, int buffsize) {
      File dbDirectory = new File(dirname);
      fm = new FileMgr(dbDirectory, blocksize);
      lm = new LogMgr(fm, LOG_FILE);
      bm = new BufferMgr(fm, lm, buffsize); 
   }

   public SimpleDB(String dirname) {
      this(dirname, BLOCK_SIZE, BUFFER_SIZE); 
      Transaction tx = newTx();
      boolean isnew = fm.isNew();
      if (isnew)
         System.out.println("creating new database");
      else {
         System.out.println("recovering existing database");
         tx.recover();
      }
      mdm = new MetadataMgr(isnew, tx);
      QueryPlanner qp = new BasicQueryPlanner(mdm);
      UpdatePlanner up = new BasicUpdatePlanner(mdm);
//    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
//    UpdatePlanner up = new IndexUpdatePlanner(mdm);
      planner = new Planner(qp, up);
      tx.commit();
   }

   public Transaction newTx() {
      return new Transaction(fm, lm, bm);
   }
   
   public MetadataMgr mdMgr() {
      return mdm;
   }
   
   public Planner planner() {
      return planner;
   }

   // These methods aid in debugging
   public FileMgr fileMgr() {
      return fm;
   }   
   public LogMgr logMgr() {
      return lm;
   }   
   public BufferMgr bufferMgr() {
      return bm;
   }   
 }
