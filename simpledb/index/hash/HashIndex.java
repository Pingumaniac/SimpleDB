package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.index.Index;
import simpledb.query.*;

public class HashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private Transaction tx;
	private String idxname;
	private Layout layout;
	private Constant searchkey = null;
	private TableScan ts = null;
	private int currentBucket = -1;

	public HashIndex(Transaction tx, String idxname, Layout layout) {
		this.tx = tx;
		this.idxname = idxname;
		this.layout = layout;
	}

	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		this.currentBucket = searchkey.hashCode() % NUM_BUCKETS;
		openBucket(currentBucket);
	}

	private void openBucket(int bucket) {
		String tblname = idxname + bucket;
		ts = new TableScan(tx, tblname, layout);
		// Move to the first record in the chain
		ts.beforeFirst();
		ts.next();
	}

	public boolean next() {
		while (true) {
			if (ts.getVal("dataval").equals(searchkey))
				return true;
			if (!ts.next()) {
				// Reached end of the current bucket chain
				int nextblk = ts.getInt("block");
				if (nextblk == -1) {
					return false; // End of chain
				}
				ts.close();
				openBucket(nextblk); // Open next block in the chain
			}
		}
	}

	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	public void insert(Constant val, int blockNum) {
		beforeFirst(val);
		if (!next() || !ts.getInt("block").equals(blockNum)) {
			ts.insert();
			ts.setInt("block", blockNum);
			ts.setVal("dataval", val);
		}
	}

	public void delete(Constant val, int blockNum) {
		beforeFirst(val);
		while (next()) {
			if (ts.getInt("block").equals(blockNum)) {
				ts.delete();
				break;
			}
		}
	}

	public void close() {
		if (ts != null)
			ts.close();
	}

	public static int searchCost(int numblocks, int rpb) {
		return numblocks / HashIndex.NUM_BUCKETS;
	}
}
