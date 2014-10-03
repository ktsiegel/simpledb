package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	/** the file represented by this HeapFile */
  private File hFile;
  private RandomAccessFile rfile;

  /** the schema of this database file */
	private TupleDesc schema;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.hFile = f;
    	try {
            this.rfile = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error in extracting as random access file.");
        }
    	this.schema = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.hFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.hFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	// Use a random access file to allow for arbitrary seeking forward and backward
    	RandomAccessFile file;
    	if (pid.getTableId() != this.getId()) throw new IllegalArgumentException();
    	try {
        // throws a FileNotFoundException if the file is not found in the filesystem
    		file = new RandomAccessFile(this.hFile, "r");
    	} catch (FileNotFoundException e) {
    		throw new IllegalArgumentException();
    	}
    	// Read in enough bytes to fill the page with data
    	byte[] pageBytes = new byte[BufferPool.PAGE_SIZE];
    	Page nPage;
    	try {
        // Read in the bytes at the correct offset to fill the byte array
    		file.seek(BufferPool.PAGE_SIZE*pid.pageNumber());
    		file.read(pageBytes, 0, BufferPool.PAGE_SIZE);
    		file.close();
    		nPage = new HeapPage((HeapPageId)pid, pageBytes);
    	} catch (IOException e) {
    		throw new IllegalArgumentException();
    	}
    	return nPage;
    }

    /**
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page page) throws IOException {
        this.rfile.seek(BufferPool.PAGE_SIZE * page.getId().pageNumber());
        this.rfile.write(page.getPageData());
        this.rfile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(this.hFile.length()/BufferPool.PAGE_SIZE);
    }

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	HeapPage page = null;
    	boolean spotsLeft = false;
    	for (int i=0; i<this.numPages(); i++) {
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
            	page.insertTuple(t);
            	spotsLeft = true;
            }
    	}
    	if (!spotsLeft) {
    		page = new HeapPage(new HeapPageId(this.getId(),this.numPages()),HeapPage.createEmptyPageData());
        	page.insertTuple(t);
        	this.writePage(page);
    	}
    	
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	pageList.add(page);
    	return pageList;
    }

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> pageList = new ArrayList<Page>();
        pageList.add(page);
        return pageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }
    
    public class HeapFileIterator implements DbFileIterator {
    	/** The transaction ID of the current transaction at the time that the heap file is iterated */
    	private TransactionId tID;

    	/** The current page of tuples being iterated through */
    	private int currPageNum;
    	
    	private int totalPages;
    	private boolean open;

    	/** The current page iterator of the page that the heap file iterator is currently on */
    	private Iterator<Tuple> currPageIterator;
    	
    	public HeapFileIterator(TransactionId tid) {
    		this.tID = tid;
    		this.currPageNum = 0;
    		this.totalPages = HeapFile.this.numPages(); 
    		this.open = false;
    	}

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
        	if (this.open) return;
        	this.open = true;
        	rewind();
        }

        /** @return true if there are more tuples available. */
        public boolean hasNext() throws DbException, TransactionAbortedException {
        	return this.open && this.currPageIterator.hasNext();
        }  

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        	if (!this.open) throw new NoSuchElementException();
        	Tuple result = this.currPageIterator.next();
        	while (!this.currPageIterator.hasNext() && this.currPageNum < this.totalPages-1) {
        		this.currPageNum++;
        		this.currPageIterator = ((HeapPage)Database.getBufferPool().getPage(tID, 
        				new HeapPageId(HeapFile.this.getId(), this.currPageNum), Permissions.READ_WRITE)).iterator();
        	}
        	return result;
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
        	this.currPageNum = 0;
        	this.currPageIterator = ((HeapPage)Database.getBufferPool().getPage(tID, 
        			new HeapPageId(HeapFile.this.getId(),this.currPageNum), Permissions.READ_WRITE)).iterator();
        }

        /**
         * Closes the iterator.
         */
        public void close() {
        	this.open = false;
        }
    }
}

