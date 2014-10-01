package simpledb;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class BPlusTreeHeaderPageTest extends SimpleDbTestBase {
	private BPlusTreePageId pid;

	public static final byte[] EXAMPLE_DATA;
	static {
		EXAMPLE_DATA = BPlusTreeHeaderPage.createEmptyPageData();
	}

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void addTable() throws Exception {
		this.pid = new BPlusTreePageId(-1, -1, BPlusTreePageId.HEADER);
		Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.getId()
	 */
	@Test public void getId() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		assertEquals(pid, page.getId());
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.getPrevPageId()
	 */
	@Test public void getPrevPageId() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		assertTrue(page.getPrevPageId() == null);
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.getNextPageId()
	 */
	@Test public void getNextPageId() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		assertTrue(page.getNextPageId() == null);
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.setPrevPageId()
	 */
	@Test public void setPrevPageId() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.HEADER);
		page.setPrevPageId(id);
		assertEquals(id, page.getPrevPageId());

		id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.INTERNAL);
		try {
			page.setPrevPageId(id);
			throw new Exception("should not be able to set prevPageId to internal node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.setNextPageId()
	 */
	@Test public void setNextPageId() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.HEADER);
		page.setNextPageId(id);
		assertEquals(id, page.getNextPageId());

		id = new BPlusTreePageId(pid.getTableId() + 1, 1, BPlusTreePageId.HEADER);
		try {
			page.setNextPageId(id);
			throw new Exception("should not be able to set nextPageId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.numSlots()
	 */
	@Test public void numSlots() throws Exception {
		assertEquals(32704, BPlusTreeHeaderPage.getNumSlots());
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.getEmptySlot()
	 */
	@Test public void getEmptySlot() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		assertEquals(0, page.getEmptySlot());
		page.init();
		assertEquals(-1, page.getEmptySlot());
		page.markSlotUsed(50, false);
		assertEquals(50, page.getEmptySlot());
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.isSlotUsed() and BPlusTreeHeaderPage.markSlotUsed()
	 */
	@Test public void getSlot() throws Exception {
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		page.init();
		for (int i = 0; i < 20; ++i) {
			page.markSlotUsed(i, false);
		}

		for (int i = 0; i < 20; i += 2) {
			page.markSlotUsed(i, true);
		}

		for (int i = 0; i < 20; ++i) {
			if(i % 2 == 0)
				assertTrue(page.isSlotUsed(i));
			else
				assertFalse(page.isSlotUsed(i));
		}

		for (int i = 20; i < 32704; ++i)
			assertTrue(page.isSlotUsed(i));

		assertEquals(1, page.getEmptySlot());
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.getPageData()
	 */
	@Test public void getPageData() throws Exception {
		BPlusTreeHeaderPage page0 = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		page0.init();
		for (int i = 0; i < 20; ++i) {
			page0.markSlotUsed(i, false);
		}

		for (int i = 0; i < 20; i += 2) {
			page0.markSlotUsed(i, true);
		}

		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, page0.getPageData());

		for (int i = 0; i < 20; ++i) {
			if(i % 2 == 0)
				assertTrue(page.isSlotUsed(i));
			else
				assertFalse(page.isSlotUsed(i));
		}

		for (int i = 20; i < 32704; ++i)
			assertTrue(page.isSlotUsed(i));

		assertEquals(1, page.getEmptySlot());
	}

	/**
	 * Unit test for BPlusTreeHeaderPage.isDirty()
	 */
	@Test public void testDirty() throws Exception {
		TransactionId tid = new TransactionId();
		BPlusTreeHeaderPage page = new BPlusTreeHeaderPage(pid, EXAMPLE_DATA);
		page.markDirty(true, tid);
		TransactionId dirtier = page.isDirty();
		assertEquals(true, dirtier != null);
		assertEquals(true, dirtier == tid);

		page.markDirty(false, tid);
		dirtier = page.isDirty();
		assertEquals(false, dirtier != null);
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BPlusTreeHeaderPageTest.class);
	}
}
