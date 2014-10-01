package simpledb;

import java.util.*;
import java.io.*;

import simpledb.Predicate.Op;

/**
 * Each instance of BPlusTreeInternalPage stores data for one page of a BPlusTreeFile and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see BPlusTreeFile
 * @see BufferPool
 *
 */
public class BPlusTreeInternalPage implements Page {
	private volatile boolean dirty = false;
	private volatile TransactionId dirtier = null;

	final static int INDEX_SIZE = Type.INT_TYPE.getLen();

	final BPlusTreePageId pid;
	final TupleDesc td;
	final byte header[];
	final Field keys[];
	final int children[];
	final int numSlots;
	final int keyField;

	private int childCategory; // either leaf or internal
	private int parent; // parent is always internal node or 0 for root node

	byte[] oldData;
	private final Byte oldDataLock=new Byte((byte)0);

	/**
	 * Create a BPlusTreeInternalPage from a set of bytes of data read from disk.
	 * The format of a BPlusTreeInternalPage is a set of header bytes indicating
	 * the slots of the page that are in use, some number of entry slots, and extra
	 * bytes for the parent pointer, one extra child pointer (a node with m entries 
	 * has m+1 pointers to children), and the category of all child pages (either 
	 * leaf or internal).
	 *  Specifically, the number of entries is equal to: <p>
	 *          floor((BufferPool.getPageSize()*8 - extra bytes*8) / (entry size * 8 + 1))
	 * <p> where entry size is the size of entries in this index node
	 * (key + child pointer), which can be determined via the key field and 
	 * {@link Catalog#getTupleDesc}.
	 * The number of 8-bit header words is equal to:
	 * <p>
	 *      ceiling((no. entry slots + 1) / 8)
	 * <p>
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#getPageSize()
	 * 
	 * @param id - the id of this page
	 * @param data - the raw data of this page
	 * @param key - the field which the index is keyed on
	 */
	public BPlusTreeInternalPage(BPlusTreePageId id, byte[] data, int key) throws IOException {
		this.pid = id;
		this.keyField = key;
		this.td = Database.getCatalog().getTupleDesc(id.getTableId());
		this.numSlots = getNumEntries() + 1;
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// Read the parent pointer
		try {
			Field f = Type.INT_TYPE.parse(dis);
			this.parent = ((IntField) f).getValue();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}

		// read the child page category
		childCategory = (int) dis.readByte();

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		keys = new Field[numSlots];
		try{
			// allocate and read the keys of this page
			// start from 1 because the first key slot is not used
			// since a node with m keys has m+1 pointers
			keys[0] = null;
			for (int i=1; i<keys.length; i++)
				keys[i] = readNextKey(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}

		children = new int[numSlots];
		try{
			// allocate and read the child pointers of this page
			for (int i=0; i<children.length; i++)
				children[i] = readNextChild(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();

		setBeforeImage();
	}

	/** Retrieve the number of entries on this page. (The number of keys)
        @return the number of entries on this page
	 */
	public int getNumEntries() {        
		int keySize = td.getFieldType(keyField).getLen();
		int bitsPerEntryIncludingHeader = keySize * 8 + INDEX_SIZE * 8 + 1;
		// extraBits are: one parent pointer, 1 byte for child page category, 
		// one extra child pointer (node with m entries has m+1 pointers to children), 1 bit for extra header
		int extraBits = 2 * INDEX_SIZE * 8 + 8 + 1; 
		int entriesPerPage = (BufferPool.getPageSize()*8 - extraBits) / bitsPerEntryIncludingHeader; //round down
		return entriesPerPage;
	}

	/**
	 * Computes the number of bytes in the header of a B+ internal page with each entry occupying entrySize bytes
	 * @return the number of bytes in the header
	 */
	private int getHeaderSize() {        
		int slotsPerPage = getNumEntries() + 1;
		int hb = (slotsPerPage / 8);
		if (hb * 8 < slotsPerPage) hb++;

		return hb;
	}

	/** Return a view of this page before it was modified
        -- used by recovery */
	public BPlusTreeInternalPage getBeforeImage(){
		try {
			byte[] oldDataRef = null;
			synchronized(oldDataLock)
			{
				oldDataRef = oldData;
			}
			return new BPlusTreeInternalPage(pid,oldDataRef,keyField);
		} catch (IOException e) {
			e.printStackTrace();
			//should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		synchronized(oldDataLock)
		{
			oldData = getPageData().clone();
		}
	}

	/**
	 * @return the PageId associated with this page.
	 */
	public BPlusTreePageId getId() {
		return pid;
	}

	/**
	 * Suck up keys from the source file.
	 */
	private Field readNextKey(DataInputStream dis, int slotId) throws NoSuchElementException {
		// if associated bit is not set, read forward to the next key, and
		// return null.
		if (!isSlotUsed(slotId)) {
			for (int i=0; i<td.getFieldType(keyField).getLen(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty key");
				}
			}
			return null;
		}

		// read the key field
		Field f = null;
		try {
			f = td.getFieldType(keyField).parse(dis);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return f;
	}

	/**
	 * Suck up child pointers from the source file.
	 */
	private int readNextChild(DataInputStream dis, int slotId) throws NoSuchElementException {
		// if associated bit is not set, read forward to the next child pointer, and
		// return -1.
		if (!isSlotUsed(slotId)) {
			for (int i=0; i<INDEX_SIZE; i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty child pointer");
				}
			}
			return -1;
		}

		// read child pointer
		int child = -1;
		try {
			Field f = Type.INT_TYPE.parse(dis);
			child = ((IntField) f).getValue();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return child;
	}

	/**
	 * Generates a byte array representing the contents of this page.
	 * Used to serialize this page to disk.
	 * <p>
	 * The invariant here is that it should be possible to pass the byte
	 * array generated by getPageData to the BPlusTreeInternalPage constructor and
	 * have it produce an identical BPlusTreeInternalPage object.
	 *
	 * @see #BPlusTreeInternalPage
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = BufferPool.getPageSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// write out the parent pointer
		try {
			dos.writeInt(parent);

		} catch (IOException e) {
			e.printStackTrace();
		}

		// write out the child page category
		try {
			dos.writeByte((byte) childCategory);

		} catch (IOException e) {
			e.printStackTrace();
		}

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the keys
		// start from 1 because the first key slot is not used
		// since a node with m keys has m+1 pointers
		for (int i=1; i<keys.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j=0; j<td.getFieldType(keyField).getLen(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			try {
				keys[i].serialize(dos);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		// create the child pointers
		for (int i=0; i<children.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j=0; j<INDEX_SIZE; j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			try {
				dos.writeInt(children[i]);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// padding
		int zerolen = BufferPool.getPageSize() - (INDEX_SIZE + 1 + header.length + 
				td.getFieldType(keyField).getLen() * (keys.length - 1) + INDEX_SIZE * children.length); 
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Static method to generate a byte array corresponding to an empty
	 * BPlusTreeInternalPage.
	 * Used to add new, empty pages to the file. Passing the results of
	 * this method to the BPlusTreeInternalPage constructor will create a BPlusTreeInternalPage with
	 * no valid entries in it.
	 *
	 * @return The returned ByteArray.
	 */
	public static byte[] createEmptyPageData() {
		int len = BufferPool.getPageSize();
		return new byte[len]; //all 0
	}

	/**
	 * Delete the specified entry (key + 1 child pointer) from the page; the entry 
	 * should be updated to reflect that it is no longer stored on any page.
	 * @throws DbException if this entry is not on this page, or entry slot is
	 *         already empty.
	 * @param e The entry to delete
	 * @param deleteRightChild - if true, delete the right child. Otherwise
	 *        delete the left child
	 */
	public void deleteEntry(BPlusTreeEntry e, boolean deleteRightChild) throws DbException {
		RecordId rid = e.getRecordId();
		if(rid == null)
			throw new DbException("tried to delete entry with null rid");
		if((rid.getPageId().pageNumber() != pid.pageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
			throw new DbException("tried to delete entry on invalid page or table");
		if (!isSlotUsed(rid.tupleno()))
			throw new DbException("tried to delete null entry.");
		if(deleteRightChild) {
			markSlotUsed(rid.tupleno(), false); 
		}
		else {
			for(int i = rid.tupleno() - 1; i >= 0; i--) {
				if(isSlotUsed(i)) {
					children[i] = children[rid.tupleno()];
					markSlotUsed(rid.tupleno(), false); 
					break;
				}	
			}
		}
		e.setRecordId(null);
	}

	/**
	 * Delete the specified entry (key + right child pointer) from the page; the entry 
	 * should be updated to reflect that it is no longer stored on any page.
	 * @throws DbException if this entry is not on this page, or entry slot is
	 *         already empty.
	 * @param e The entry to delete
	 */
	public void deleteEntry(BPlusTreeEntry e) throws DbException {
		deleteEntry(e, true);
	}

	/**
	 * Adds the specified entry to the page; the entry should be updated to reflect
	 *  that it is now stored on this page.
	 * @throws DbException if the page is full (no empty slots) or key field type,
	 *         table id, or child page category is a mismatch, or the entry is invalid
	 * @param e The entry to add.
	 */
	public void insertEntry(BPlusTreeEntry e) throws DbException {
		if (!e.getKey().getType().equals(td.getFieldType(keyField)))
			throw new DbException("key field type mismatch, in insertEntry");

		if(e.getLeftChild().getTableId() != pid.getTableId() || e.getRightChild().getTableId() != pid.getTableId())
			throw new DbException("table id mismatch in insertEntry");

		if(childCategory == 0) {
			if(e.getLeftChild().pgcateg() != e.getRightChild().pgcateg())
				throw new DbException("child page category mismatch in insertEntry");

			childCategory = e.getLeftChild().pgcateg();
		}
		else if(e.getLeftChild().pgcateg() != childCategory || e.getRightChild().pgcateg() != childCategory)
			throw new DbException("child page category mismatch in insertEntry");

		// if this is the first entry, add it and return
		if(getNumEmptySlots() == getNumEntries()) {
			children[0] = e.getLeftChild().pageNumber();
			children[1] = e.getRightChild().pageNumber();
			keys[1] = e.getKey();
			markSlotUsed(0, true);
			markSlotUsed(1, true);
			return;
		}

		// find the first empty slot, starting from 1
		int emptySlot = -1;
		for (int i=1; i<numSlots; i++) {
			if (!isSlotUsed(i)) {
				emptySlot = i;
				break;
			}
		}

		if (emptySlot == -1)
			throw new DbException("called insertEntry on page with no empty slots.");        

		// find the child pointer matching the left or right child in this entry
		int lessOrEqKey = -1;
		for (int i=0; i<numSlots; i++) {
			if(isSlotUsed(i)) {
				if(children[i] == e.getLeftChild().pageNumber() || children[i] == e.getRightChild().pageNumber()) {
					if(i > 0 && keys[i].compare(Op.GREATER_THAN, e.getKey())) {
						throw new DbException("attempt to insert invalid entry with left child " + 
								e.getLeftChild().pageNumber() + ", right child " + 
								e.getRightChild().pageNumber() + " and key " + e.getKey());
					}
					lessOrEqKey = i;
					if(children[i] == e.getRightChild().pageNumber()) {
						children[i] = e.getLeftChild().pageNumber();
					}
				}
				else if(lessOrEqKey != -1) {
					// validate that the next key is greater than or equal to the one we are inserting
					if(keys[i].compare(Op.LESS_THAN, e.getKey())) {
						throw new DbException("attempt to insert invalid entry with left child " + 
								e.getLeftChild().pageNumber() + ", right child " + 
								e.getRightChild().pageNumber() + " and key " + e.getKey());
					}
					break;
				}
			}
		}

		if(lessOrEqKey == -1) {
			throw new DbException("attempt to insert invalid entry with left child " + 
					e.getLeftChild().pageNumber() + ", right child " + 
					e.getRightChild().pageNumber() + " and key " + e.getKey());
		}

		// shift entries back or forward to fill empty slot and make room for new entry
		// while keeping entries in sorted order
		int goodSlot = -1;
		if(emptySlot < lessOrEqKey) {
			for(int i = emptySlot; i < lessOrEqKey; i++) {
				moveEntry(i+1, i);
			}
			goodSlot = lessOrEqKey;
		}
		else {
			for(int i = emptySlot; i > lessOrEqKey + 1; i--) {
				moveEntry(i-1, i);
			}
			goodSlot = lessOrEqKey + 1;
		}

		// insert new entry into the correct spot in sorted order
		markSlotUsed(goodSlot, true);
		Debug.log(1, "BPlusTreeLeafPage.insertEntry: new entry, tableId = %d pageId = %d slotId = %d", pid.getTableId(), pid.pageNumber(), goodSlot);
		keys[goodSlot] = e.getKey();
		children[goodSlot] = e.getRightChild().pageNumber();
		e.setRecordId(new RecordId(pid, goodSlot));
	}

	/**
	 * Move an entry from one slot to another slot, and update the corresponding
	 * headers
	 */
	private void moveEntry(int from, int to) {
		if(!isSlotUsed(to) && isSlotUsed(from)) {
			markSlotUsed(to, true);
			keys[to] = keys[from];
			children[to] = children[from];
			markSlotUsed(from, false);
		}
	}

	/**
	 * Get the parent id of this page
	 * @return the parent id
	 */
	public BPlusTreePageId getParentId() {
		if(parent == 0) {
			return BPlusTreeRootPtrPage.getId(pid.getTableId());
		}
		return new BPlusTreePageId(pid.getTableId(), parent, BPlusTreePageId.INTERNAL);
	}

	/**
	 * Set the parent id
	 * @param id - the id of the parent of this page
	 * @throws DbException if the id is not valid
	 */
	public void setParentId(BPlusTreePageId id) throws DbException {
		if(id == null) {
			throw new DbException("parent id must not be null");
		}
		if(id.getTableId() != pid.getTableId()) {
			throw new DbException("table id mismatch in setParentId");
		}
		if(id.pgcateg() != BPlusTreePageId.INTERNAL && id.pgcateg() != BPlusTreePageId.ROOT_PTR) {
			throw new DbException("parent must be an internal node or root pointer");
		}
		if(id.pgcateg() == BPlusTreePageId.ROOT_PTR) {
			parent = 0;
		}
		else {
			parent = id.pageNumber();
		}
	}

	/**
	 * Marks this page as dirty/not dirty and record that transaction
	 * that did the dirtying
	 */
	public void markDirty(boolean dirty, TransactionId tid) {
		this.dirty = dirty;
		if (dirty) this.dirtier = tid;
	}

	/**
	 * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
	 */
	public TransactionId isDirty() {
		if (this.dirty)
			return this.dirtier;
		else
			return null;
	}

	/**
	 * Returns the number of empty slots on this page.
	 */
	public int getNumEmptySlots() {
		int cnt = 0;
		// start from 1 because the first key slot is not used
		// since a node with m keys has m+1 pointers
		for(int i=1; i<numSlots; i++)
			if(!isSlotUsed(i))
				cnt++;
		return cnt;
	}

	/**
	 * Returns true if associated slot on this page is filled.
	 */
	public boolean isSlotUsed(int i) {
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;
		return (header[headerbyte] & (1 << headerbit)) != 0;
	}

	/**
	 * Abstraction to fill or clear a slot on this page.
	 */
	private void markSlotUsed(int i, boolean value) {
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;

		Debug.log(1, "BPlusTreeInternalPage.setSlot: setting slot %d to %b", i, value);
		if(value)
			header[headerbyte] |= 1 << headerbit;
		else
			header[headerbyte] &= (0xFF ^ (1 << headerbit));
	}

	/**
	 * @return an iterator over all entries on this page (calling remove on this iterator throws an UnsupportedOperationException)
	 * (note that this iterator shouldn't return entries in empty slots!)
	 */
	public Iterator<BPlusTreeEntry> iterator() {
		return new BPlusTreeInternalPageIterator(this);
	}

	/**
	 * protected method used by the iterator to get the ith key out of this page
	 * @param i - the index of the key
	 * @return the ith key
	 * @throws NoSuchElementException
	 */
	Field getKey(int i) throws NoSuchElementException {

		// key at slot 0 is not used
		if (i <= 0 || i >= keys.length)
			throw new NoSuchElementException();

		try {
			if(!isSlotUsed(i)) {
				Debug.log(1, "BPlusTreeInternalPage.getKey: slot %d in %d:%d is not used", i, pid.getTableId(), pid.pageNumber());
				return null;
			}

			Debug.log(1, "BPlusTreeInternalPage.getKey: returning key %d", i);
			return keys[i];

		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * protected method used by the iterator to get the ith child page id out of this page
	 * @param i - the index of the child page id
	 * @return the ith child page id
	 * @throws NoSuchElementException
	 */
	BPlusTreePageId getChildId(int i) throws NoSuchElementException {

		if (i < 0 || i >= children.length)
			throw new NoSuchElementException();

		try {
			if(!isSlotUsed(i)) {
				Debug.log(1, "BPlusTreeInternalPage.getChildId: slot %d in %d:%d is not used", i, pid.getTableId(), pid.pageNumber());
				return null;
			}

			Debug.log(1, "BPlusTreeInternalPage.getChildId: returning child id %d", i);
			return new BPlusTreePageId(pid.getTableId(), children[i], childCategory);

		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}
}

/**
 * Helper class that implements the Java Iterator for entries on a BPlusTreeInternalPage.
 */
class BPlusTreeInternalPageIterator implements Iterator<BPlusTreeEntry> {
	int curEntry = 1;
	BPlusTreePageId prevChildId = null;
	BPlusTreeEntry nextToReturn = null;
	BPlusTreeInternalPage p;

	public BPlusTreeInternalPageIterator(BPlusTreeInternalPage p) {
		this.p = p;
	}

	public boolean hasNext() {
		if (nextToReturn != null)
			return true;

		try {
			if(prevChildId == null) {
				prevChildId = p.getChildId(0);
				if(prevChildId == null) {
					return false;
				}
			}
			while (true) {
				int entry = curEntry++;
				Field key = p.getKey(entry);
				BPlusTreePageId childId = p.getChildId(entry);
				if(key != null && childId != null) {
					nextToReturn = new BPlusTreeEntry(key, prevChildId, childId);
					nextToReturn.setRecordId(new RecordId(p.pid, entry));
					prevChildId = childId;
					return true;
				}
			}
		} catch(NoSuchElementException e) {
			return false;
		}
	}

	public BPlusTreeEntry next() {
		BPlusTreeEntry next = nextToReturn;

		if (next == null) {
			if (hasNext()) {
				next = nextToReturn;
				nextToReturn = null;
				return next;
			} else
				throw new NoSuchElementException();
		} else {
			nextToReturn = null;
			return next;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
