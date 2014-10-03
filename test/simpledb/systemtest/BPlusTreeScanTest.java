package simpledb.systemtest;

import simpledb.systemtest.SystemTestUtil;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Iterator;

import org.junit.Test;
import org.junit.Before;

import simpledb.*;
import simpledb.Predicate.Op;

/**
 * Dumps the contents of a table.
 * args[1] is the number of columns.  E.g., if it's 5, then BPlusTreeScanTest will end
 * up dumping the contents of f4.0.txt.
 */
public class BPlusTreeScanTest extends SimpleDbTestBase {
    private final static Random r = new Random();
    
    /** Tests the scan operator for a table with the specified dimensions. */
    private void validateScan(int[] columnSizes, int[] rowSizes)
            throws IOException, DbException, TransactionAbortedException {
    	TransactionId tid = new TransactionId();
    	for (int columns : columnSizes) {
    		int keyField = r.nextInt(columns);
            for (int rows : rowSizes) {
                ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
                BPlusTreeFile f = BPlusTreeUtility.createRandomBPlusTreeFile(columns, rows, null, tuples, keyField);
                BPlusTreeScan scan = new BPlusTreeScan(tid, f.getId(), "table", null);
                SystemTestUtil.matchTuples(scan, tuples);
                Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
            }
        }
    	Database.getBufferPool().transactionComplete(tid);
    }
    
    // comparator to sort Tuples by key field
    private static class TupleComparator implements Comparator<ArrayList<Integer>> {
        private int keyField;
        
        public TupleComparator(int keyField) {
        	this.keyField = keyField;
        }
        
    	public int compare(ArrayList<Integer> t1, ArrayList<Integer> t2) {
            int cmp = 0;
            if(t1.get(keyField) < t2.get(keyField)) {
            	cmp = -1;
            }
            else if(t1.get(keyField) > t2.get(keyField)) {
            	cmp = 1;
            }
            return cmp;
        }
    }
    
    /** Counts the number of readPage operations. */
    class InstrumentedBPlusTreeFile extends BPlusTreeFile {
        public InstrumentedBPlusTreeFile(File f, int keyField, TupleDesc td) {
            super(f, keyField, td);
        }

        @Override
        public Page readPage(PageId pid) throws NoSuchElementException {
            readCount += 1;
            return super.readPage(pid);
        }

        public int readCount = 0;
    }
    
    /** Scan 1-4 columns. */
    @Test public void testSmall() throws IOException, DbException, TransactionAbortedException {
        int[] columnSizes = new int[]{1, 2, 3, 4};
        int[] rowSizes =
                new int[]{0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + r.nextInt(4096)};
        validateScan(columnSizes, rowSizes);
    }

    /** Test that rewinding a BPlusTreeScan iterator works. */
    @Test public void testRewind() throws IOException, DbException, TransactionAbortedException {
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = r.nextInt(2);
        BPlusTreeFile f = BPlusTreeUtility.createRandomBPlusTreeFile(2, 1000, null, tuples, keyField);
        Collections.sort(tuples, new TupleComparator(keyField));
        
        TransactionId tid = new TransactionId();
        BPlusTreeScan scan = new BPlusTreeScan(tid, f.getId(), "table", null);
        scan.open();
        for (int i = 0; i < 100; ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (int i = 0; i < 100; ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    /** Test that rewinding a BPlusTreeScan iterator works with predicates. */
    @Test public void testRewindPredicates() throws IOException, DbException, TransactionAbortedException {
    	// Create the table
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = r.nextInt(3);
        BPlusTreeFile f = BPlusTreeUtility.createRandomBPlusTreeFile(3, 1000, null, tuples, keyField);
        Collections.sort(tuples, new TupleComparator(keyField));
                
        // EQUALS
        TransactionId tid = new TransactionId();
        ArrayList<ArrayList<Integer>> tuplesFiltered = new ArrayList<ArrayList<Integer>>();
        IndexPredicate ipred = new IndexPredicate(Op.EQUALS, new IntField(r.nextInt(BPlusTreeUtility.MAX_RAND_VALUE)));
        Iterator<ArrayList<Integer>> it = tuples.iterator();
        while(it.hasNext()) {
        	ArrayList<Integer> tup = it.next();
        	if(tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        BPlusTreeScan scan = new BPlusTreeScan(tid, f.getId(), "table", ipred);
        scan.open();
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }
        scan.close();
        
        // LESS_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.LESS_THAN, new IntField(r.nextInt(BPlusTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	ArrayList<Integer> tup = it.next();
        	if(tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        scan = new BPlusTreeScan(tid, f.getId(), "table", ipred);
        scan.open();
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }
        scan.close();
        
        // GREATER_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(BPlusTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	ArrayList<Integer> tup = it.next();
        	if(tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        scan = new BPlusTreeScan(tid, f.getId(), "table", ipred);
        scan.open();
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
    
    /** Test that scanning the BPlusTree for predicates does not read all the pages */
    @Test public void testReadPage() throws Exception {
    	// Create the table
        final int LEAF_PAGES = 30;
    	
    	ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = 0;
        BPlusTreeFile f = BPlusTreeUtility.createBPlusTreeFile(2, LEAF_PAGES*502, null, tuples, keyField);
        Collections.sort(tuples, new TupleComparator(keyField));
        TupleDesc td = Utility.getTupleDesc(2);
        InstrumentedBPlusTreeFile table = new InstrumentedBPlusTreeFile(f.getFile(), keyField, td);
        Database.getCatalog().addTable(table, SystemTestUtil.getUUID());
        
        // EQUALS
        TransactionId tid = new TransactionId();
        ArrayList<ArrayList<Integer>> tuplesFiltered = new ArrayList<ArrayList<Integer>>();
        IndexPredicate ipred = new IndexPredicate(Op.EQUALS, new IntField(r.nextInt(LEAF_PAGES*502)));
        Iterator<ArrayList<Integer>> it = tuples.iterator();
        while(it.hasNext()) {
        	ArrayList<Integer> tup = it.next();
        	if(tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        BPlusTreeScan scan = new BPlusTreeScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(scan, tuplesFiltered);
        // root pointer page + root + leaf page (possibly 2 leaf pages)
        assertTrue(table.readCount == 3 || table.readCount == 4);
        
        // LESS_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.LESS_THAN, new IntField(r.nextInt(LEAF_PAGES*502)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	ArrayList<Integer> tup = it.next();
        	if(tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        scan = new BPlusTreeScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(scan, tuplesFiltered);
        // root pointer page + root + leaf pages
        int leafPageCount = tuplesFiltered.size()/502;
        if(leafPageCount < LEAF_PAGES)
        	leafPageCount++; // +1 for next key locking
        assertEquals(leafPageCount + 2, table.readCount);
        
        // GREATER_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(LEAF_PAGES*502)));
        it = tuples.iterator();
        while(it.hasNext()) {
        	ArrayList<Integer> tup = it.next();
        	if(tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
        		tuplesFiltered.add(tup);
        	}
        }
        
        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        scan = new BPlusTreeScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(scan, tuplesFiltered);
        // root pointer page + root + leaf pages
        leafPageCount = tuplesFiltered.size()/502;
        if(leafPageCount < LEAF_PAGES)
        	leafPageCount++; // +1 for next key locking
        assertEquals(leafPageCount + 2, table.readCount);
        
        Database.getBufferPool().transactionComplete(tid);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BPlusTreeScanTest.class);
    }
}
