package simpledb.file;

import java.io.*;
import java.util.*;

public class FileMgr {
   private File dbDirectory;
   private int blocksize;
   private boolean isNew;
   private Map<String, RandomAccessFile> openFiles = new HashMap<>();

   // Ex 3.15 Counters for the number of blocks read and written.
   private long blocksRead;
   private long blocksWritten;

   public FileMgr(File dbDirectory, int blocksize) {
      this.dbDirectory = dbDirectory;
      this.blocksize = blocksize;
      isNew = !dbDirectory.exists();

      if (isNew)
         dbDirectory.mkdirs();

      for (String filename : dbDirectory.list())
         if (filename.startsWith("temp"))
            new File(dbDirectory, filename).delete();

      // Ex 3.15 Initialize counters
      blocksRead = 0;
      blocksWritten = 0;
   }

   public synchronized void read(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());
         f.seek(blk.number() * blocksize);
         f.getChannel().read(p.contents());
         blocksRead++; // Increment read counter
      } catch (IOException e) {
         throw new RuntimeException("cannot read block " + blk);
      }
   }

   public synchronized void write(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());
         f.seek(blk.number() * blocksize);
         f.getChannel().write(p.contents());
         blocksWritten++; // Increment write counter
      } catch (IOException e) {
         throw new RuntimeException("cannot write block " + blk);
      }
   }

   public synchronized BlockId append(String filename) {
      int newblknum = length(filename);
      BlockId blk = new BlockId(filename, newblknum);
      byte[] b = new byte[blocksize];
      try {
         RandomAccessFile f = getFile(filename);
         f.seek(blk.number() * blocksize);
         f.write(b);
         blocksWritten++; // Increment write counter
      } catch (IOException e) {
         throw new RuntimeException("cannot append block " + blk);
      }
      return blk;
   }

   public int length(String filename) {
      try {
         RandomAccessFile f = getFile(filename);
         return (int) (f.length() / blocksize);
      } catch (IOException e) {
         throw new RuntimeException("cannot access " + filename);
      }
   }

   public boolean isNew() {
      return isNew;
   }

   public int blockSize() {
      return blocksize;
   }

   private RandomAccessFile getFile(String filename) throws IOException {
      RandomAccessFile f = openFiles.get(filename);
      if (f == null) {
         File dbTable = new File(dbDirectory, filename);
         f = new RandomAccessFile(dbTable, "rws");
         openFiles.put(filename, f);
      }
      return f;
   }

   // Ex 3.15 Methods to get statistics.
   public long getBlocksRead() {
      return blocksRead;
   }

   public long getBlocksWritten() {
      return blocksWritten;
   }
}
