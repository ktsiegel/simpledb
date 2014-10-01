package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.Predicate.Op;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class BPlusTreeFileDeleteTest extends SimpleDbTestBase {
	private BPlusTreeFile f;
	private TransactionId tid;

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before
	public void setUp() throws Exception {
		f = BPlusTreeUtility.createRandomBPlusTreeFile(2, 20, null, null, 0);
		tid = new TransactionId();
	}

	@After
	public void tearDown() throws Exception {
		Database.getBufferPool().transactionComplete(tid);
	}

	/**
	 * Unit test for BPlusTreeFile.deleteTuple()
	 */
	@Test public void deleteTuple() throws Exception {
		DbFileIterator it = f.iterator(tid);
		it.open();
		while(it.hasNext()) {
			Tuple t = it.next();
			f.deleteTuple(tid, t);
		}
		it.rewind();
		assertFalse(it.hasNext());

		// insert a couple of tuples
		f.insertTuple(tid, BPlusTreeUtility.getBPlusTreeTuple(5, 2));
		f.insertTuple(tid, BPlusTreeUtility.getBPlusTreeTuple(17, 2));

		it.rewind();
		assertTrue(it.hasNext());

	}

	@Test
	public void testRedistributeLeafPages() throws Exception {
		// This should create a B+ tree with two partially full leaf pages
		BPlusTreeFile twoLeafPageFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 600,
				null, null, 0);

		// Delete some tuples from the first page until it gets to minimum occupancy
		DbFileIterator it = twoLeafPageFile.iterator(tid);
		it.open();
		int count = 0;
		while(it.hasNext() && count < 49) {
			Tuple t = it.next();
			BPlusTreePageId pid = (BPlusTreePageId) t.getRecordId().getPageId();
			BPlusTreeLeafPage p = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
					tid, pid, Permissions.READ_ONLY);
			assertEquals(202 + count, p.getNumEmptySlots());
			twoLeafPageFile.deleteTuple(tid, t);
			count++;
		}

		// deleting a tuple now should bring the page below minimum occupancy and cause 
		// the tuples to be redistributed
		Tuple t = it.next();
		it.close();
		BPlusTreePageId pid = (BPlusTreePageId) t.getRecordId().getPageId();
		BPlusTreeLeafPage p = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
				tid, pid, Permissions.READ_ONLY);
		assertEquals(251, p.getNumEmptySlots());
		twoLeafPageFile.deleteTuple(tid, t);
		assertTrue(p.getNumEmptySlots() <= 251);

		BPlusTreePageId rightSiblingId = p.getRightSiblingId();
		BPlusTreeLeafPage rightSibling = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
				tid, rightSiblingId, Permissions.READ_ONLY);
		assertTrue(rightSibling.getNumEmptySlots() > 202);
	} 

	@Test
	public void testMergeLeafPages() throws Exception {
		// This should create a B+ tree with one full page and two half-full leaf pages
		BPlusTreeFile threeLeafPageFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 1005,
				null, null, 0);

		// there should be one internal node and 3 leaf nodes
		assertEquals(4, threeLeafPageFile.numPages());

		// delete the last two tuples
		DbFileIterator it = threeLeafPageFile.iterator(tid);
		it.open();
		Tuple secondToLast = null;
		Tuple last = null;
		while(it.hasNext()) {
			secondToLast = last;
			last = it.next();
		}
		it.close();
		threeLeafPageFile.deleteTuple(tid, secondToLast);
		threeLeafPageFile.deleteTuple(tid, last);

		// confirm that the last two pages have merged successfully
		BPlusTreePageId rootPtrId = BPlusTreeRootPtrPage.getId(threeLeafPageFile.getId());
		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, rootPtrId, Permissions.READ_ONLY);
		BPlusTreeInternalPage root = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(502, root.getNumEmptySlots());
		BPlusTreeEntry e = root.iterator().next();
		BPlusTreeLeafPage leftChild = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
				tid, e.getLeftChild(), Permissions.READ_ONLY);
		BPlusTreeLeafPage rightChild = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
				tid, e.getRightChild(), Permissions.READ_ONLY);
		assertEquals(0, leftChild.getNumEmptySlots());
		assertEquals(1, rightChild.getNumEmptySlots());
		assertTrue(e.getKey().equals(rightChild.iterator().next().getField(0)));

	}

	@Test
	public void testDeleteRootPage() throws Exception {
		// This should create a B+ tree with two half-full leaf pages
		BPlusTreeFile twoLeafPageFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 503,
				null, null, 0);

		// there should be one internal node and 2 leaf nodes
		assertEquals(3, twoLeafPageFile.numPages());

		// delete the first two tuples
		DbFileIterator it = twoLeafPageFile.iterator(tid);
		it.open();
		Tuple first = it.next();
		Tuple second = it.next();
		it.close();
		twoLeafPageFile.deleteTuple(tid, first);
		twoLeafPageFile.deleteTuple(tid, second);

		// confirm that the last two pages have merged successfully and replaced the root
		BPlusTreePageId rootPtrId = BPlusTreeRootPtrPage.getId(twoLeafPageFile.getId());
		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, rootPtrId, Permissions.READ_ONLY);
		assertTrue(rootPtr.getRootId().pgcateg() == BPlusTreePageId.LEAF);
		BPlusTreeLeafPage root = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(1, root.getNumEmptySlots());
		assertTrue(root.getParentId().equals(rootPtrId));
	}

	@Test
	public void testReuseDeletedPages() throws Exception {
		// this should create a B+ tree with 3 leaf nodes
		BPlusTreeFile threeLeafPageFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 1005,
				null, null, 0);

		// 3 leaf pages, 1 internal page
		assertEquals(4, threeLeafPageFile.numPages());

		// delete enough tuples to ensure one page gets deleted
		DbFileIterator it = threeLeafPageFile.iterator(tid);
		it.open();
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}
		it.close();

		// now there should be 2 leaf pages, 1 internal page, 1 unused leaf page, 1 header page
		assertEquals(5, threeLeafPageFile.numPages());

		// insert enough tuples to ensure one of the leaf pages splits
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().insertTuple(tid, threeLeafPageFile.getId(), 
					BPlusTreeUtility.getBPlusTreeTuple(i, 2));
		}

		// now there should be 3 leaf pages, 1 internal page, and 1 header page
		assertEquals(5, threeLeafPageFile.numPages());
	}

	@Test
	public void testRedistributeInternalPages() throws Exception {
		// This should create a B+ tree with two nodes in the second tier
		// and 602 nodes in the third tier
		BPlusTreeFile bf = BPlusTreeUtility.createRandomBPlusTreeFile(2, 302204,
				null, null, 0);

		Database.resetBufferPool(500); // we need more pages for this test

		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BPlusTreeRootPtrPage.getId(bf.getId()), Permissions.READ_ONLY);
		BPlusTreeInternalPage root = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(502, root.getNumEmptySlots());

		BPlusTreeEntry rootEntry = root.iterator().next();
		BPlusTreeInternalPage leftChild = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootEntry.getLeftChild(), Permissions.READ_ONLY);
		BPlusTreeInternalPage rightChild = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootEntry.getRightChild(), Permissions.READ_ONLY);

		// delete from the right child to test redistribution from the left
		Iterator<BPlusTreeEntry> it = rightChild.iterator();
		int count = 0;
		// bring the right internal page to minimum occupancy
		while(it.hasNext() && count < 49 * 502 + 1) {
			BPlusTreeLeafPage leaf = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid, 
					it.next().getLeftChild(), Permissions.READ_ONLY);
			Tuple t = leaf.iterator().next();
			Database.getBufferPool().deleteTuple(tid, t);
			it = rightChild.iterator();
			count++;
		}

		// deleting a page of tuples should bring the internal page below minimum 
		// occupancy and cause the entries to be redistributed
		assertEquals(252, rightChild.getNumEmptySlots());
		count = 0;
		while(it.hasNext() && count < 502) {
			BPlusTreeLeafPage leaf = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid, 
					it.next().getLeftChild(), Permissions.READ_ONLY);
			Tuple t = leaf.iterator().next();
			Database.getBufferPool().deleteTuple(tid, t);
			it = rightChild.iterator();
			count++;
		}
		assertTrue(leftChild.getNumEmptySlots() > 203);
		assertTrue(rightChild.getNumEmptySlots() <= 252);

		// sanity check that the entries make sense
		BPlusTreeEntry lastLeftEntry = null;
		it = leftChild.iterator();
		while(it.hasNext()) {
			lastLeftEntry = it.next();
		}
		rootEntry = root.iterator().next();
		BPlusTreeEntry firstRightEntry = rightChild.iterator().next();
		assertTrue(lastLeftEntry.getKey().compare(Op.LESS_THAN_OR_EQ, rootEntry.getKey()));
		assertTrue(rootEntry.getKey().compare(Op.LESS_THAN_OR_EQ, firstRightEntry.getKey()));
	}

	@Test
	public void testDeleteInternalPages() throws Exception {
		// This should create a B+ tree with three nodes in the second tier
		// and 1010 nodes in the third tier
		BPlusTreeFile bigFile = BPlusTreeUtility.createRandomBPlusTreeFile(2, 506519,
				null, null, 0);

		Database.resetBufferPool(1500); // we need more pages for this test

		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BPlusTreeRootPtrPage.getId(bigFile.getId()), Permissions.READ_ONLY);
		BPlusTreeInternalPage root = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(501, root.getNumEmptySlots());

		BPlusTreeEntry e = root.iterator().next();
		BPlusTreeInternalPage leftChild = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getLeftChild(), Permissions.READ_ONLY);
		BPlusTreeInternalPage rightChild = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getRightChild(), Permissions.READ_ONLY);

		// Delete tuples causing leaf pages to merge until the first internal page 
		// gets to minimum occupancy
		DbFileIterator it = bigFile.iterator(tid);
		it.open();
		int count = 0;
		Database.getBufferPool().deleteTuple(tid, it.next());
		it.rewind();
		while(count < 252) {
			assertEquals(count, leftChild.getNumEmptySlots());
			for(int i = 0; i < 502; ++i) {
				Database.getBufferPool().deleteTuple(tid, it.next());
				it.rewind();
			}
			count++;
		}

		// deleting a page of tuples should bring the internal page below minimum 
		// occupancy and cause the entries to be redistributed
		assertEquals(252, leftChild.getNumEmptySlots());
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}
		assertEquals(252, leftChild.getNumEmptySlots());
		assertEquals(252, rightChild.getNumEmptySlots());

		// deleting another page of tuples should bring the page below minimum occupancy 
		// again but this time cause it to merge with its right sibling 
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}

		// confirm that the pages have merged
		assertEquals(502, root.getNumEmptySlots());
		e = root.iterator().next();
		leftChild = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getLeftChild(), Permissions.READ_ONLY);
		rightChild = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, e.getRightChild(), Permissions.READ_ONLY);
		assertEquals(1, leftChild.getNumEmptySlots());
		assertTrue(e.getKey().compare(Op.LESS_THAN_OR_EQ, rightChild.iterator().next().getKey()));

		// Delete tuples causing leaf pages to merge until the first internal page 
		// gets below minimum occupancy and causes the entries to be redistributed
		count = 1;
		while(count < 253) {
			assertEquals(count, leftChild.getNumEmptySlots());
			for(int i = 0; i < 502; ++i) {
				Database.getBufferPool().deleteTuple(tid, it.next());
				it.rewind();
			}
			count++;
		}

		// deleting another page of tuples should bring the page below minimum occupancy 
		// and cause it to merge with the right sibling to replace the root
		for(int i = 0; i < 502; ++i) {
			Database.getBufferPool().deleteTuple(tid, it.next());
			it.rewind();
		}

		// confirm that the last two internal pages have merged successfully and 
		// replaced the root
		BPlusTreePageId rootPtrId = BPlusTreeRootPtrPage.getId(bigFile.getId());
		rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, rootPtrId, Permissions.READ_ONLY);
		assertTrue(rootPtr.getRootId().pgcateg() == BPlusTreePageId.INTERNAL);
		root = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
				tid, rootPtr.getRootId(), Permissions.READ_ONLY);
		assertEquals(1, root.getNumEmptySlots());
		assertTrue(root.getParentId().equals(rootPtrId));

		it.close();
	}    

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BPlusTreeFileDeleteTest.class);
	}
}
