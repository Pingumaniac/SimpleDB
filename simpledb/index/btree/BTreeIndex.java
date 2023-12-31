package simpledb.index.btree;

import static java.sql.Types.INTEGER;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.index.Index;
import simpledb.query.Constant;

public class BTreeIndex implements Index {
   private Transaction tx;
   private Layout dirLayout, leafLayout;
   private String leaftbl;
   private BTreeLeaf leaf = null;
   private BlockId rootblk;

   public BTreeIndex(Transaction tx, String idxname, Layout leafLayout) {
      this.tx = tx;
      // deal with the leaves
      leaftbl = idxname + "leaf";
      this.leafLayout = leafLayout;
      if (tx.size(leaftbl) == 0) {
         BlockId blk = tx.append(leaftbl);
         BTPage node = new BTPage(tx, blk, leafLayout);
         node.format(blk, -1);
      }

      // deal with the directory
      Schema dirsch = new Schema();
      dirsch.add("block",   leafLayout.schema());
      dirsch.add("dataval", leafLayout.schema());
      String dirtbl = idxname + "dir";
      dirLayout = new Layout(dirsch);
      rootblk = new BlockId(dirtbl, 0);
      if (tx.size(dirtbl) == 0) {
         // create new root block
         tx.append(dirtbl);
         BTPage node = new BTPage(tx, rootblk, dirLayout);
         node.format(rootblk, 0);
         // insert initial directory entry
         int fldtype = dirsch.type("dataval");
         Constant minval = (fldtype == INTEGER) ?
               new Constant(Integer.MIN_VALUE) :
               new Constant("");
         node.insertDir(0, minval, 0);
         node.close();
      }
   }

   public void beforeFirst(Constant searchkey) {
      close();
      BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
      int blknum = root.search(searchkey);
      root.close();
      BlockId leafblk = new BlockId(leaftbl, blknum);
      leaf = new BTreeLeaf(tx, leafblk, leafLayout, searchkey);
   }

   public boolean next() {
      return leaf.next();
   }

   public RID getDataRid() {
      return leaf.getDataRid();
   }

   public void insert(Constant dataval, RID datarid) {
      BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);

      // Split the root if it's full
      if (root.isFull()) {
         splitRoot(root);
         root = new BTreeDir(tx, rootblk, dirLayout);
      }

      // Recursively insert and split full blocks
      insert(root, dataval, datarid);
   }

   private void insert(BTreeDir dir, Constant val, RID rid) {
      DirEntry e = dir.search(val);
      if (e == null) {
         BTreeLeaf leaf = new BTreeLeaf(tx, leaftbl, leafLayout, -1);
         leaf.insert(val, rid);
         leaf.close();
      } else {
         BlockId childblk = new BlockId(filename, e.blockNumber());
         BTreeDir child = new BTreeDir(tx, childblk, dirLayout);
         if (child.isFull()) {
            split(child);
            // Re-search after splitting
            child = new BTreeDir(tx, childblk, dirLayout);
         }
         insert(child, val, rid);
         child.close();
      }
      dir.close();
   }

   private void splitRoot(BTreeDir root) {
      BlockId firstblk = root.split(0, new DirEntry(null, -1));
      DirEntry e = new DirEntry(root.getDataVal(0), firstblk.number());
      root.insertDir(0, e);
      root.close();
   }

   private void split(BTreeDir dir) {
      int level = dir.getLevel();
      DirEntry e = dir.split(0, new DirEntry(null, -1));

      if (level == 0)
         e = new DirEntry(e.dataVal(), dir.getCurrentBlock().number());

      dir.close();
      BTreeDir parent = findParent(dir.getCurrentBlock());
      parent.insert(e);
      parent.close();
   }

   public void delete(Constant dataval, RID datarid) {
      beforeFirst(dataval);
      leaf.delete(datarid);
      leaf.close();
   }

   public void close() {
      if (leaf != null)
         leaf.close();
   }

   public static int searchCost(int numblocks, int rpb) {
      return 1 + (int)(Math.log(numblocks) / Math.log(rpb));
   }
}
