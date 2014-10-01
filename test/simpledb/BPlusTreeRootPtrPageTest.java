package simpledb;

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

public class BPlusTreeRootPtrPageTest extends SimpleDbTestBase {
	private BPlusTreePageId pid;

	public static final byte[] EXAMPLE_DATA;
	static {
		// Identify the root page and page category
		int root = 1;
		int rootCategory = BPlusTreePageId.LEAF;
		int header = 2;

		// Convert it to a BPlusTreeRootPtrPage
		try {
			EXAMPLE_DATA = BPlusTreeFileEncoder.convertToRootPtrPage(root, rootCategory, header);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before public void addTable() throws Exception {
		this.pid = new BPlusTreePageId(-1, 0, BPlusTreePageId.ROOT_PTR);
		Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
	}

	/**
	 * Unit test for BPlusTreeRootPtrPage.getId()
	 */
	@Test public void getId() throws Exception {
		BPlusTreeRootPtrPage page = new BPlusTreeRootPtrPage(pid, EXAMPLE_DATA);
		assertEquals(pid, page.getId());
	}

	/**
	 * Unit test for BPlusTreeRootPtrPage.getRootId()
	 */
	@Test public void getRootId() throws Exception {
		BPlusTreeRootPtrPage page = new BPlusTreeRootPtrPage(pid, EXAMPLE_DATA);
		assertEquals(new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.LEAF), page.getRootId());
	}

	/**
	 * Unit test for BPlusTreeRootPtrPage.setRootId()
	 */
	@Test public void setRootId() throws Exception {
		BPlusTreeRootPtrPage page = new BPlusTreeRootPtrPage(pid, EXAMPLE_DATA);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.INTERNAL);
		page.setRootId(id);
		assertEquals(id, page.getRootId());

		id = new BPlusTreePageId(pid.getTableId(), 1, BPlusTreePageId.ROOT_PTR);
		try {
			page.setRootId(id);
			throw new Exception("should not be able to set rootId to RootPtr node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}

		id = new BPlusTreePageId(pid.getTableId() + 1, 1, BPlusTreePageId.INTERNAL);
		try {
			page.setRootId(id);
			throw new Exception("should not be able to set rootId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeRootPtrPage.getHeaderId()
	 */
	@Test public void getHeaderId() throws Exception {
		BPlusTreeRootPtrPage page = new BPlusTreeRootPtrPage(pid, EXAMPLE_DATA);
		assertEquals(new BPlusTreePageId(pid.getTableId(), 2, BPlusTreePageId.HEADER), page.getHeaderId());
	}

	/**
	 * Unit test for BPlusTreeRootPtrPage.setHeaderId()
	 */
	@Test public void setHeaderId() throws Exception {
		BPlusTreeRootPtrPage page = new BPlusTreeRootPtrPage(pid, EXAMPLE_DATA);
		BPlusTreePageId id = new BPlusTreePageId(pid.getTableId(), 3, BPlusTreePageId.HEADER);
		page.setHeaderId(id);
		assertEquals(id, page.getHeaderId());

		id = new BPlusTreePageId(pid.getTableId(), 2, BPlusTreePageId.ROOT_PTR);
		try {
			page.setHeaderId(id);
			throw new Exception("should not be able to set headerId to RootPtr node; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}

		id = new BPlusTreePageId(pid.getTableId() + 1, 1, BPlusTreePageId.HEADER);
		try {
			page.setHeaderId(id);
			throw new Exception("should not be able to set rootId to a page from a different table; expected DbException");
		} catch (DbException e) {
			// explicitly ignored
		}
	}

	/**
	 * Unit test for BPlusTreeRootPtrPage.isDirty()
	 */
	@Test public void testDirty() throws Exception {
		TransactionId tid = new TransactionId();
		BPlusTreeRootPtrPage page = new BPlusTreeRootPtrPage(pid, EXAMPLE_DATA);
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
		return new JUnit4TestAdapter(BPlusTreeRootPtrPageTest.class);
	}
}
