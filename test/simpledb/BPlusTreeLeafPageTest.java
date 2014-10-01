package simpledb;

import simpledb.BPlusTreeFileEncoder.TupleComparator;
import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

//import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class BPlusTreeLeafPageTest extends SimpleDbTestBase {
	private BPlusTreePageId pid;

	public static final int[][] EXAMPLE_VALUES = new int[][] {
		{ 31933, 862 },
		{ 29402, 56883 },
		{ 1468, 5825 },
		{ 17876, 52278 },
		{ 6350, 36090 },
		{ 34784, 43771 },
		{ 28617, 56874 },
		{ 19209, 23253 },
		{ 56462, 24979 },
		{ 51440, 56685 },
		{ 3596, 62307 },
		{ 45569, 2719 },
		{ 22064, 43575 },
		{ 42812, 44947 },
		{ 22189, 19724 },
		{ 33549, 36554 },
		{ 9086, 53184 },
		{ 42878, 33394 },
		{ 62778, 21122 },
		{ 17197, 16388 }
	};

	public static final byte[] EXAMPLE_DATA;
	static {
		// Build the input table
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for (int[] tuple : EXAMPLE_VALUES) {
			Tuple tup = new Tuple(Utility.getTupleDesc(2));
			for (int i = 0; i < tuple.length; i++) {
				tup.setField(i, new IntField(tuple[i]));
			}
			tuples.add(tup);
		}

		// Convert it to a BPlusTreeLeafPage
		try {
			EXAMPLE_DATA = BPlusTreeFileEncoder.convertToLeafPage(tuples, 
					BufferPool.getPageSize(), 2, new Type[]{Type.INT_TYPE, Type.INT_TYPE}, 0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void addTable() throws Exception {
		this.pid = new BPlusTreePageId(-1, -1, BPlusTreePageId.LEAF);
		Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
	}

	/**
	 * Unit test for BPlusTreeLeafPage.getId()
	 */
	@Test public void getId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		assertEquals(pid, page.getId());
	}

	/**
	 * Unit test for BPlusTreeLeafPage.getParentId()
	 */
	@Test public void getParentId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		assertEquals(new BPlusTreePageId(pid.getTableId(), 0, BPlusTreePageId.ROOT_PTR), page.getParentId());
	}

	/**
	 * Unit test for BPlusTreeLeafPage.getLeftSiblingId()
	 */
	@Test public void getLeftSiblingId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		assertTrue(page.getLeftSiblingId() == null);
	}

	/**
	 * Unit test for BPlusTreeLeafPage.getRightSiblingId()
	 */
	@Test public void getRightSiblingId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		assertTrue(page.getRightSiblingId() == null);
	}

	/**
	 * Unit test for BPlusTreeLeafPage.setParentId()
	 */
	@Test public void setParentId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.INTERNAL);
		page.setParentId(id);
		assertEquals(id, page.getParentId());

		id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.LEAF);
		try {
			page.setParentId(id);
			throw new Exception("should not be able to set parentId to leaf node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeLeafPage.setLeftSiblingId()
	 */
	@Test public void setLeftSiblingId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.LEAF);
		page.setLeftSiblingId(id);
		assertEquals(id, page.getLeftSiblingId());

		id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.INTERNAL);
		try {
			page.setLeftSiblingId(id);
			throw new Exception("should not be able to set leftSiblingId to internal node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeLeafPage.setRightSiblingId()
	 */
	@Test public void setRightSiblingId() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.LEAF);
		page.setRightSiblingId(id);
		assertEquals(id, page.getRightSiblingId());

		id = new BPlusTreePageId(pid.getTableId() + 1, 1, BPlusTreePageId.LEAF);
		try {
			page.setRightSiblingId(id);
			throw new Exception("should not be able to set rightSiblingId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeLeafPage.iterator()
	 */
	@Test public void testIterator() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		Iterator<Tuple> it = page.iterator();

		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for (int[] tuple : EXAMPLE_VALUES) {
			Tuple tup = new Tuple(Utility.getTupleDesc(2));
			for (int i = 0; i < tuple.length; i++) {
				tup.setField(i, new IntField(tuple[i]));
			}
			tuples.add(tup);
		}
		Collections.sort(tuples, new TupleComparator(0));

		int row = 0;
		while (it.hasNext()) {
			Tuple tup = it.next();

			assertEquals(tuples.get(row).getField(0), tup.getField(0));
			assertEquals(tuples.get(row).getField(1), tup.getField(1));
			row++;
		}
	}

	/**
	 * Unit test for BPlusTreeLeafPage.getNumEmptySlots()
	 */
	@Test public void getNumEmptySlots() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		assertEquals(482, page.getNumEmptySlots());
	}

	/**
	 * Unit test for BPlusTreeLeafPage.isSlotUsed()
	 */
	@Test public void getSlot() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);

		for (int i = 0; i < 20; ++i)
			assertTrue(page.isSlotUsed(i));

		for (int i = 20; i < 502; ++i)
			assertFalse(page.isSlotUsed(i));
	}

	/**
	 * Unit test for BPlusTreeLeafPage.isDirty()
	 */
	@Test public void testDirty() throws Exception {
		TransactionId tid = new TransactionId();
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		page.markDirty(true, tid);
		TransactionId dirtier = page.isDirty();
		assertEquals(true, dirtier != null);
		assertEquals(true, dirtier == tid);

		page.markDirty(false, tid);
		dirtier = page.isDirty();
		assertEquals(false, dirtier != null);
	}

	/**
	 * Unit test for BPlusTreeLeafPage.addTuple()
	 */
	@Test public void addTuple() throws Exception {
		// create two blank pages -- one keyed on the first field, 
		// the second keyed on the second field
		byte[] data = BPlusTreeLeafPage.createEmptyPageData();
		BPlusTreeLeafPage page0 = new BPlusTreeLeafPage(pid, data, 0);
		BPlusTreeLeafPage page1 = new BPlusTreeLeafPage(pid, data, 1);

		// insert tuples into both pages
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for (int[] tuple : EXAMPLE_VALUES) {
			Tuple tup = new Tuple(Utility.getTupleDesc(2));
			for (int i = 0; i < tuple.length; i++) {
				tup.setField(i, new IntField(tuple[i]));
			}
			tuples.add(tup);
			page0.insertTuple(tup);
			page1.insertTuple(tup);
		}

		// check that the tuples are ordered by field 0 in page0
		Collections.sort(tuples, new TupleComparator(0));
		Iterator<Tuple> it0 = page0.iterator();
		for(Tuple tup : tuples) {
			assertTrue(tup.equals(it0.next()));
		}

		// check that the tuples are ordered by field 1 in page1
		Collections.sort(tuples, new TupleComparator(1));
		Iterator<Tuple> it1 = page1.iterator();
		for(Tuple tup : tuples) {
			assertTrue(tup.equals(it1.next()));
		}

		// now insert tuples until the page fills up
		int free = page0.getNumEmptySlots();

		// NOTE(ghuo): this nested loop existence check is slow, but it
		// shouldn't make a difference for n = 502 slots.

		for (int i = 0; i < free; ++i) {
			Tuple addition = BPlusTreeUtility.getBPlusTreeTuple(i, 2);
			page0.insertTuple(addition);
			assertEquals(free-i-1, page0.getNumEmptySlots());

			// loop through the iterator to ensure that the tuple actually exists
			// on the page
			Iterator<Tuple> it = page0.iterator();
			boolean found = false;
			while (it.hasNext()) {
				Tuple tup = it.next();
				if (TestUtil.compareTuples(addition, tup)) {
					found = true;

					// verify that the RecordId is sane
					assertTrue(page0.getId().equals(tup.getRecordId().getPageId()));
					break;
				}
			}
			assertTrue(found);
		}

		// now, the page should be full.
		try {
			page0.insertTuple(BPlusTreeUtility.getBPlusTreeTuple(0, 2));
			throw new Exception("page should be full; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeLeafPage.deleteTuple() with false tuples
	 */
	@Test(expected=DbException.class)
	public void deleteNonexistentTuple() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		page.deleteTuple(BPlusTreeUtility.getBPlusTreeTuple(2, 2));
	}

	/**
	 * Unit test for BPlusTreeLeafPage.deleteTuple()
	 */
	@Test public void deleteTuple() throws Exception {
		BPlusTreeLeafPage page = new BPlusTreeLeafPage(pid, EXAMPLE_DATA, 0);
		int free = page.getNumEmptySlots();

		// first, build a list of the tuples on the page.
		Iterator<Tuple> it = page.iterator();
		LinkedList<Tuple> tuples = new LinkedList<Tuple>();
		while (it.hasNext())
			tuples.add(it.next());
		Tuple first = tuples.getFirst();

		// now, delete them one-by-one from both the front and the end.
		int deleted = 0;
		while (tuples.size() > 0) {
			page.deleteTuple(tuples.removeFirst());
			page.deleteTuple(tuples.removeLast());
			deleted += 2;
			assertEquals(free + deleted, page.getNumEmptySlots());
		}

		// now, the page should be empty.
		try {
			page.deleteTuple(first);
			throw new Exception("page should be empty; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BPlusTreeLeafPageTest.class);
	}
}
