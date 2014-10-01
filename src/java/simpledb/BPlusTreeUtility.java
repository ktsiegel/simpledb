package simpledb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import simpledb.Predicate.Op;

/** Helper methods used for testing and implementing random features. */
public class BPlusTreeUtility {

	public static final int MAX_RAND_VALUE = 1 << 16;

	public static ArrayList<Integer> tupleToList(Tuple tuple) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < tuple.getTupleDesc().numFields(); ++i) {
            int value = ((IntField)tuple.getField(i)).getValue();
            list.add(value);
        }
        return list;
    }
	
	/**
	 * @return a Tuple with a single IntField with value n and with
	 *   RecordId(BPlusTreePageId(1,2, BPlusTreePageId.LEAF), 3)
	 */
	public static Tuple getBPlusTreeTuple(int n) {
		Tuple tup = new Tuple(Utility.getTupleDesc(1));
		tup.setRecordId(new RecordId(new BPlusTreePageId(1, 2, BPlusTreePageId.LEAF), 3));
		tup.setField(0, new IntField(n));
		return tup;
	}

	/**
	 * @return a Tuple with an IntField for every element of tupdata
	 *   and RecordId(BPlusTreePageId(1, 2, BPlusTreePageId.LEAF), 3)
	 */
	public static Tuple getBPlusTreeTuple(int[] tupdata) {
		Tuple tup = new Tuple(Utility.getTupleDesc(tupdata.length));
		tup.setRecordId(new RecordId(new BPlusTreePageId(1, 2, BPlusTreePageId.LEAF), 3));
		for (int i = 0; i < tupdata.length; ++i)
			tup.setField(i, new IntField(tupdata[i]));
		return tup;
	}

	/**
	 * @return a Tuple with a 'width' IntFields each with value n and
	 *   with RecordId(BPlusTreePageId(1, 2, BPlusTreePageId.LEAF), 3)
	 */
	public static Tuple getBPlusTreeTuple(int n, int width) {
		Tuple tup = new Tuple(Utility.getTupleDesc(width));
		tup.setRecordId(new RecordId(new BPlusTreePageId(1, 2, BPlusTreePageId.LEAF), 3));
		for (int i = 0; i < width; ++i)
			tup.setField(i, new IntField(n));
		return tup;
	}

	/**
	 * @return a BPlusTreeEntry with an IntField with value n and with
	 *   RecordId(BPlusTreePageId(1,2, BPlusTreePageId.INTERNAL), 3)
	 */
	public static BPlusTreeEntry getBPlusTreeEntry(int n) {
		BPlusTreePageId leftChild = new BPlusTreePageId(1, n, BPlusTreePageId.LEAF);
		BPlusTreePageId rightChild = new BPlusTreePageId(1, n+1, BPlusTreePageId.LEAF);
		BPlusTreeEntry e = new BPlusTreeEntry(new IntField(n), leftChild, rightChild);
		e.setRecordId(new RecordId(new BPlusTreePageId(1, 2, BPlusTreePageId.INTERNAL), 3));
		return e;
	}

	/**
	 * @return a BPlusTreeEntry with an IntField with value n and with
	 *   RecordId(BPlusTreePageId(tableid,2, BPlusTreePageId.INTERNAL), 3)
	 */
	public static BPlusTreeEntry getBPlusTreeEntry(int n, int tableid) {
		BPlusTreePageId leftChild = new BPlusTreePageId(tableid, n, BPlusTreePageId.LEAF);
		BPlusTreePageId rightChild = new BPlusTreePageId(tableid, n+1, BPlusTreePageId.LEAF);
		BPlusTreeEntry e = new BPlusTreeEntry(new IntField(n), leftChild, rightChild);
		e.setRecordId(new RecordId(new BPlusTreePageId(tableid, 2, BPlusTreePageId.INTERNAL), 3));
		return e;
	}

	/**
	 * @return a BPlusTreeEntry with an IntField with value key and with
	 *   RecordId(BPlusTreePageId(tableid,2, BPlusTreePageId.INTERNAL), 3)
	 */
	public static BPlusTreeEntry getBPlusTreeEntry(int n, int key, int tableid) {
		BPlusTreePageId leftChild = new BPlusTreePageId(tableid, n, BPlusTreePageId.LEAF);
		BPlusTreePageId rightChild = new BPlusTreePageId(tableid, n+1, BPlusTreePageId.LEAF);
		BPlusTreeEntry e = new BPlusTreeEntry(new IntField(key), leftChild, rightChild);
		e.setRecordId(new RecordId(new BPlusTreePageId(tableid, 2, BPlusTreePageId.INTERNAL), 3));
		return e;
	}

	/** @param columnSpecification Mapping between column index and value. */
	public static BPlusTreeFile createRandomBPlusTreeFile(
			int columns, int rows, Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples, int keyField)
					throws IOException, DbException, TransactionAbortedException {
		return createRandomBPlusTreeFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples, keyField);
	}

	/**
	 * Generates a random B+ tree file for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param maxValue - the maximum random value in this B+ tree
	 * @param columnSpecification - optional column specification
	 * @param tuples - optional list of tuples to return
	 * @param keyField - the index of the key field
	 * @return a BPlusTreeFile
	 * @throws IOException
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public static BPlusTreeFile createRandomBPlusTreeFile(int columns, int rows,
			int maxValue, Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples, int keyField) 
					throws IOException, DbException, TransactionAbortedException {
		if (tuples != null) {
			tuples.clear();
		} else {
			tuples = new ArrayList<ArrayList<Integer>>(rows);
		}

		Random r = new Random();

		// Fill the tuples list with generated values
		for (int i = 0; i < rows; ++i) {
			ArrayList<Integer> tuple = new ArrayList<Integer>(columns);
			for (int j = 0; j < columns; ++j) {
				// Generate random values, or use the column specification
				Integer columnValue = null;
				if (columnSpecification != null) columnValue = columnSpecification.get(j);
				if (columnValue == null) {
					columnValue = r.nextInt(maxValue);
				}
				tuple.add(columnValue);
			}
			tuples.add(tuple);
		}

		// Convert the tuples list to a B+ tree file
		File hFile = File.createTempFile("table", ".dat");
		hFile.deleteOnExit();

		File bFile = File.createTempFile("table_index", ".dat");
		bFile.deleteOnExit();

		Type[] typeAr = new Type[columns];
		Arrays.fill(typeAr, Type.INT_TYPE);
		return BPlusTreeFileEncoder.convert(tuples, hFile, bFile, BufferPool.getPageSize(),
				columns, typeAr, ',', keyField) ;
	}

	/**
	 * creates a *non* random B+ tree file for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param columnSpecification - optional column specification
	 * @param tuples - optional list of tuples to return
	 * @param keyField - the index of the key field
	 * @return a BPlusTreeFile
	 * @throws IOException
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public static BPlusTreeFile createBPlusTreeFile(int columns, int rows,
			Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples, int keyField) 
					throws IOException, DbException, TransactionAbortedException {
		if (tuples != null) {
			tuples.clear();
		} else {
			tuples = new ArrayList<ArrayList<Integer>>(rows);
		}

		// Fill the tuples list with generated values
		for (int i = 0; i < rows; ++i) {
			ArrayList<Integer> tuple = new ArrayList<Integer>(columns);
			for (int j = 0; j < columns; ++j) {
				// Generate values, or use the column specification
				Integer columnValue = null;
				if (columnSpecification != null) columnValue = columnSpecification.get(j);
				if (columnValue == null) {
					columnValue = (i+1)*(j+1);
				}
				tuple.add(columnValue);
			}
			tuples.add(tuple);
		}

		// Convert the tuples list to a B+ tree file
		File hFile = File.createTempFile("table", ".dat");
		hFile.deleteOnExit();

		File bFile = File.createTempFile("table_index", ".dat");
		bFile.deleteOnExit();

		Type[] typeAr = new Type[columns];
		Arrays.fill(typeAr, Type.INT_TYPE);
		return BPlusTreeFileEncoder.convert(tuples, hFile, bFile, BufferPool.getPageSize(),
				columns, typeAr, ',', keyField) ;
	}

	/** Opens a BPlusTreeFile and adds it to the catalog.
	 *
	 * @param cols number of columns in the table.
	 * @param f location of the file storing the table.
	 * @param keyField the field the B+ tree is keyed on
	 * @return the opened table.
	 */
	public static BPlusTreeFile openBPlusTreeFile(int cols, File f, int keyField) {
		// create the BPlusTreeFile and add it to the catalog
		TupleDesc td = Utility.getTupleDesc(cols);
		BPlusTreeFile bf = new BPlusTreeFile(f, keyField, td);
		Database.getCatalog().addTable(bf, UUID.randomUUID().toString());
		return bf;
	}

	public static BPlusTreeFile openBPlusTreeFile(int cols, String colPrefix, File f, int keyField) {
		// create the BPlusTreeFile and add it to the catalog
		TupleDesc td = Utility.getTupleDesc(cols, colPrefix);
		BPlusTreeFile bf = new BPlusTreeFile(f, keyField, td);
		Database.getCatalog().addTable(bf, UUID.randomUUID().toString());
		return bf;
	}

	/**
	 * A utility method to create a new BPlusTreeFile with no data,
	 * assuming the path does not already exist. If the path exists, the file
	 * will be overwritten. The new table will be added to the Catalog with
	 * the specified number of columns as IntFields indexed on the keyField.
	 */
	public static BPlusTreeFile createEmptyBPlusTreeFile(String path, int cols, int keyField)
			throws IOException {
		File f = new File(path);
		// touch the file
		FileOutputStream fos = new FileOutputStream(f);
		fos.write(new byte[0]);
		fos.close();

		BPlusTreeFile bf = openBPlusTreeFile(cols, f, keyField);

		return bf;
	}

	/**
	 * Helper class that attempts to insert a tuple in a new thread
	 *
	 * @return a handle to the Thread that will attempt insertion after it
	 *   has been started
	 */
	static class BPlusTreeWriter extends Thread {

		TransactionId tid;
		BPlusTreeFile bf;
		int item;
		int count;
		boolean success;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param tid the transaction on whose behalf we want to insert the tuple
		 * @param bf the B+ tree file into which we want to insert the tuple
		 * @param item the key of the tuple to insert
		 * @param count the number of times to insert the tuple
		 */
		public BPlusTreeWriter(TransactionId tid, BPlusTreeFile bf, int item, int count) {
			this.tid = tid;
			this.bf = bf;
			this.item = item;
			this.count = count;
			this.success = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}

		public void run() {
			try {
				int c = 0;
				while(c < count) {
					Tuple t = BPlusTreeUtility.getBPlusTreeTuple(item, 2);
					Database.getBufferPool().insertTuple(tid, bf.getId(), t);

					IndexPredicate ipred = new IndexPredicate(Op.EQUALS, t.getField(bf.keyField()));
					DbFileIterator it = bf.indexIterator(tid, ipred);
					it.open();
					c = 0;
					while(it.hasNext()) {
						it.next();
						c++;
					}
					it.close();
				}
				synchronized(slock) {
					success = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
				synchronized(elock) {
					error = e;
				}

				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				}
			}
		}

		/**
		 * @return true if we successfully inserted the tuple
		 */
		 public boolean succeeded() {
			 synchronized(slock) {
				 return success;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while inserting the tuple;
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}

	/**
	 * Helper class that searches for tuple(s) in a new thread
	 *
	 * @return a handle to the Thread that will attempt to search for tuple(s) after it
	 *   has been started
	 */
	static class BPlusTreeReader extends Thread {

		TransactionId tid;
		BPlusTreeFile bf;
		Field f;
		int count;
		boolean found;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param tid the transaction on whose behalf we want to search for the tuple(s)
		 * @param bf the B+ tree file containing the tuple(s)
		 * @param f the field to search for
		 * @param count the number of tuples to search for
		 */
		public BPlusTreeReader(TransactionId tid, BPlusTreeFile bf, Field f, int count) {
			this.tid = tid;
			this.bf = bf;
			this.f = f;
			this.count = count;
			this.found = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}

		public void run() {
			try {
				while(true) {
					IndexPredicate ipred = new IndexPredicate(Op.EQUALS, f);
					DbFileIterator it = bf.indexIterator(tid, ipred);
					it.open();
					int c = 0;
					while(it.hasNext()) {
						it.next();
						c++;
					}
					it.close();
					if(c >= count) {
						synchronized(slock) {
							found = true;
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				synchronized(elock) {
					error = e;
				}

				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				}
			}
		}

		/**
		 * @return true if we successfully found the tuple(s)
		 */
		 public boolean found() {
			 synchronized(slock) {
				 return found;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while searching for the tuple(s);
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}
	
	/**
	 * Helper class that attempts to insert a tuple in a new thread
	 *
	 * @return a handle to the Thread that will attempt insertion after it
	 *   has been started
	 */
	public static class BPlusTreeInserter extends Thread {

		TransactionId tid;
		BPlusTreeFile bf;
		int[] tupdata;
		BlockingQueue<ArrayList<Integer>> insertedTuples;
		boolean success;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param bf the B+ tree file into which we want to insert the tuple
		 * @param tupdata the data of the tuple to insert
		 * @param the list of tuples that were successfully inserted
		 */
		public BPlusTreeInserter(BPlusTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, tupdata, insertedTuples);
		}

		public void run() {
			try {
				Tuple t = BPlusTreeUtility.getBPlusTreeTuple(tupdata);
				Database.getBufferPool().insertTuple(tid, bf.getId(), t);
				Database.getBufferPool().transactionComplete(tid);
				ArrayList<Integer> tuple = tupleToList(t);
				insertedTuples.put(tuple);
				synchronized(slock) {
					success = true;
				}
			} catch (Exception e) {
				if(!(e instanceof TransactionAbortedException)) {
					e.printStackTrace();
				}
				synchronized(elock) {
					error = e;
				}

				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				}
			}
		}
		
		private void init(BPlusTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			this.tid = new TransactionId();
			this.bf = bf;
			this.tupdata = tupdata;
			this.insertedTuples = insertedTuples;
			this.success = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}
		
		public void rerun(BPlusTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, tupdata, insertedTuples);
			run();
		}

		/**
		 * @return true if we successfully inserted the tuple
		 */
		 public boolean succeeded() {
			 synchronized(slock) {
				 return success;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while inserting the tuple;
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}
    
	/**
	 * Helper class that attempts to delete tuple(s) in a new thread
	 *
	 * @return a handle to the Thread that will attempt deletion after it
	 *   has been started
	 */
	public static class BPlusTreeDeleter extends Thread {

		TransactionId tid;
		BPlusTreeFile bf;
		BlockingQueue<ArrayList<Integer>> insertedTuples;
		ArrayList<Integer> tuple;
		boolean success;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param bf the B+ tree file from which we want to delete the tuple(s)
		 * @param the list of tuples to delete
		 */
		public BPlusTreeDeleter(BPlusTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, insertedTuples);
		}

		public void run() {
			try {
				tuple = insertedTuples.take();
				if(bf.getTupleDesc().numFields() != tuple.size()) {
					throw new DbException("tuple desc mismatch");
				}
				IntField key = new IntField(tuple.get(bf.keyField()));
				IndexPredicate ipred = new IndexPredicate(Op.EQUALS, key);
				DbFileIterator it = bf.indexIterator(tid, ipred);
				it.open();
				while(it.hasNext()) {
					Tuple t = it.next();
					if(tupleToList(t).equals(tuple)) {
						Database.getBufferPool().deleteTuple(tid, t);
						break;
					}
				}
				it.close();
				Database.getBufferPool().transactionComplete(tid);
				synchronized(slock) {
					success = true;
				}
			} catch (Exception e) {
				if(!(e instanceof TransactionAbortedException)) {
					e.printStackTrace();
				}
				synchronized(elock) {
					error = e;
				}

				try {
					insertedTuples.put(tuple);
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				} catch (InterruptedException e3) {
					e3.printStackTrace();
				}
			}
		}
		
		private void init(BPlusTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			this.tid = new TransactionId();
			this.bf = bf;
			this.insertedTuples = insertedTuples;
			this.success = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}
		
		public void rerun(BPlusTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, insertedTuples);
			run();
		}

		/**
		 * @return true if we successfully inserted the tuple
		 */
		 public boolean succeeded() {
			 synchronized(slock) {
				 return success;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while inserting the tuple;
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}

}

