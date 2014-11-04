package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.BPlusTreeUtility.BPlusTreeWriter;
import simpledb.Predicate.Op;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class BPlusTreeFileReadTest extends SimpleDbTestBase {
	private BPlusTreeFile f;
	private TransactionId tid;
	private TupleDesc td;

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before
	public void setUp() throws Exception {
		f = BPlusTreeUtility.createRandomBPlusTreeFile(2, 20, null, null, 0);
		td = Utility.getTupleDesc(2);
		tid = new TransactionId();
	}

	@After
	public void tearDown() throws Exception {
		Database.getBufferPool().transactionComplete(tid);
	}

	/**
	 * Unit test for BPlusTreeFile.getId()
	 */
	@Test
	public void getId() throws Exception {
		int id = f.getId();

		// NOTE(ghuo): the value could be anything. test determinism, at least.
		assertEquals(id, f.getId());
		assertEquals(id, f.getId());

		BPlusTreeFile other = BPlusTreeUtility.createRandomBPlusTreeFile(1, 1, null, null, 0);
		assertTrue(id != other.getId());
	}

	/**
	 * Unit test for BPlusTreeFile.getTupleDesc()
	 */
	@Test
	public void getTupleDesc() throws Exception {    	
		assertEquals(td, f.getTupleDesc());        
	}
	/**
	 * Unit test for BPlusTreeFile.numPages()
	 */
	@Test
	public void numPages() throws Exception {
		assertEquals(1, f.numPages());
	}

	/**
	 * Unit test for BPlusTreeFile.readPage()
	 */
	@Test
	public void readPage() throws Exception {
		BPlusTreePageId rootPtrPid = new BPlusTreePageId(f.getId(), 0, BPlusTreePageId.ROOT_PTR);
		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) f.readPage(rootPtrPid);

		assertEquals(1, rootPtr.getRootId().pageNumber());
		assertEquals(BPlusTreePageId.LEAF, rootPtr.getRootId().pgcateg());

		BPlusTreePageId pid = new BPlusTreePageId(f.getId(), 1, BPlusTreePageId.LEAF);
		BPlusTreeLeafPage page = (BPlusTreeLeafPage) f.readPage(pid);

		// NOTE(ghuo): we try not to dig too deeply into the Page API here; we
		// rely on BPlusTreePageTest for that. perform some basic checks.
		assertEquals(482, page.getNumEmptySlots());
		assertTrue(page.isSlotUsed(1));
		assertFalse(page.isSlotUsed(20));
	}

	@Test
	public void testIteratorBasic() throws Exception {
		BPlusTreeFile smallFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 3, null,
				null, 0);

		DbFileIterator it = smallFile.iterator(tid);
		// Not open yet
		assertFalse(it.hasNext());
		try {
			it.next();
			fail("expected exception");
		} catch (NoSuchElementException e) {
		}

		it.open();
		int count = 0;
		while (it.hasNext()) {
			assertNotNull(it.next());
			count += 1;
		}
		assertEquals(3, count);
		it.close();
	}

	@Test
	public void testIteratorClose() throws Exception {
		// make more than 1 page. Previous closed iterator would start fetching
		// from page 1.
		BPlusTreeFile twoLeafPageFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 520,
				null, null, 0);

		// there should be 3 pages - two leaf pages and one internal page (the root)
		assertEquals(3, twoLeafPageFile.numPages());

		DbFileIterator it = twoLeafPageFile.iterator(tid);
		it.open();
		assertTrue(it.hasNext());
		it.close();
		try {
			it.next();
			fail("expected exception");
		} catch (NoSuchElementException e) {
		}
		// close twice is harmless
		it.close();
	}

	/**
	 * Unit test for BPlusTreeFile.indexIterator()
	 */
	@Test public void indexIterator() throws Exception {
		BPlusTreeFile twoLeafPageFile = BPlusTreeUtility.createBPlusTreeFile(2, 520,
				null, null, 0);
		Field f =  new IntField(5);

		// greater than
		IndexPredicate ipred = new IndexPredicate(Op.GREATER_THAN, f);
		DbFileIterator it = twoLeafPageFile.indexIterator(tid, ipred);
		it.open();
		int count = 0;
		while(it.hasNext()) {
			Tuple t = it.next();
			assertTrue(t.getField(0).compare(Op.GREATER_THAN, f));
			count++;
		}
		assertEquals(515, count);
		it.close();

		// less than or equal to
		ipred = new IndexPredicate(Op.LESS_THAN_OR_EQ, f);
		it = twoLeafPageFile.indexIterator(tid, ipred);
		it.open();
		count = 0;
		while(it.hasNext()) {
			Tuple t = it.next();
			assertTrue(t.getField(0).compare(Op.LESS_THAN_OR_EQ, f));
			count++;
		}
		assertEquals(5, count);
		it.close();

		// equal to
		ipred = new IndexPredicate(Op.EQUALS, f);
		it = twoLeafPageFile.indexIterator(tid, ipred);
		it.open();
		count = 0;
		while(it.hasNext()) {
			Tuple t = it.next();
			assertTrue(t.getField(0).compare(Op.EQUALS, f));
			count++;
		}
		assertEquals(1, count);
		it.close();

		// now insert a record and ensure EQUALS returns both records
		twoLeafPageFile.insertTuple(tid, BPlusTreeUtility.getBPlusTreeTuple(5, 2));
		ipred = new IndexPredicate(Op.EQUALS, f);
		it = twoLeafPageFile.indexIterator(tid, ipred);
		it.open();
		count = 0;
		while(it.hasNext()) {
			Tuple t = it.next();
			assertTrue(t.getField(0).compare(Op.EQUALS, f));
			count++;
		}
		assertEquals(2, count);
		it.close();

		// search for a non-existent record
		f = new IntField(1000);
		ipred = new IndexPredicate(Op.GREATER_THAN, f);
		it = twoLeafPageFile.indexIterator(tid, ipred);
		it.open();
		assertFalse(it.hasNext());
		it.close();

	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BPlusTreeFileReadTest.class);
	}
}
