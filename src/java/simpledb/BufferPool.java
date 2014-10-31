package simpledb;

import java.io.*;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Maps page IDs to actual Pages; holds all pages currently in buffer pool. */
    private ConcurrentHashMap<PageId, Page> pages;
    private ArrayList<PageId> lruCache;
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> pageLockTransactions;
    private ConcurrentHashMap<PageId, ArrayList<Lock>> sharedLocks;
    private ConcurrentHashMap<PageId, Lock> exclusiveLocks;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int pageTotal;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	pageTotal = numPages;
    	pages = new ConcurrentHashMap<PageId, Page>();
    	lruCache = new ArrayList<PageId>();
    	pageLockTransactions = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    	sharedLocks = new ConcurrentHashMap<PageId, ArrayList<Lock>>();
    	exclusiveLocks = new ConcurrentHashMap<PageId, Lock>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	if (perm == Permissions.READ_ONLY) {
    		handleReadOnlyCase(tid, pid, perm);
    	} else if (perm == Permissions.READ_WRITE) {
    		handleReadWriteCase(tid, pid, perm);
    	}
    	return getPageSynchronized(tid, pid, perm);
    }
    
    public void handleReadOnlyCase(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	if (pageLockTransactions.containsKey(pid)) {
			// A lock currently exists on this page.
			if (exclusiveLocks.containsKey(pid)) {
				// If it's an exclusive lock, then check if it's within the same transaction.
				try {
					exclusiveLocks.get(pid).tryLock(100, TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					throw new TransactionAbortedException();
				} finally {
					if (!exclusiveLocks.get(pid).tryLock()) {
						throw new TransactionAbortedException();
					}
					exclusiveLocks.get(pid).lock();
				}
				if (!sharedLocks.contains(pid)) {
					sharedLocks.put(pid, new ArrayList<Lock>());
				}
				sharedLocks.get(pid).add(exclusiveLocks.remove(pid));
				if (!pageLockTransactions.get(pid).contains(tid)) {
					pageLockTransactions.get(pid).add(tid);
				}
			} else { // It's ok if the lock is a shared lock
				pageLockTransactions.get(pid).add(tid);
			}
		} else { 
			// No lock on this page, so create a new lock and lock it.
			if (!sharedLocks.contains(pid)) {
				sharedLocks.put(pid, new ArrayList<Lock>());
			}
			Lock lock = new ReentrantLock();
			lock.lock();
			sharedLocks.get(pid).add(lock);
			// Update which transaction holds a lock on this page.
			if (!pageLockTransactions.contains(pid)) {
				pageLockTransactions.put(pid, new ArrayList<TransactionId>());
			}
			pageLockTransactions.get(pid).add(tid);
		}
    }
    
    public void handleReadWriteCase(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	if (pageLockTransactions.containsKey(pid)) {
			// A lock exists on this page.
			if (!exclusiveLocks.containsKey(pid) && sharedLocks.containsKey(pid)) {
				// If it is an exclusive lock, then update the lock accordingly.
				for (Lock lock : sharedLocks.get(pid)) {
					try {
						lock.tryLock(100, TimeUnit.MILLISECONDS);
					} catch (Exception e) {
						throw new TransactionAbortedException();
					} finally {
						if (!lock.tryLock()) {
							throw new TransactionAbortedException();
						}
						lock.lock();
					}
				}
				sharedLocks.remove(pid);
				pageLockTransactions.remove(pid);
				Lock lock = new ReentrantLock();
				lock.lock();
				exclusiveLocks.put(pid, lock);
				pageLockTransactions.put(pid, new ArrayList<TransactionId>());
				pageLockTransactions.get(pid).add(tid);
			}
			// Acquire the exclusive lock.
			if (exclusiveLocks.get(pid) != null) {
				System.out.println("Trying to get read write lock: " + exclusiveLocks.get(pid).toString() + " in transaction " + tid.toString());
				try {
					exclusiveLocks.get(pid).tryLock(100, TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					throw new TransactionAbortedException();
				} finally {
					if (!exclusiveLocks.get(pid).tryLock()) {
						throw new TransactionAbortedException();
					}
					exclusiveLocks.get(pid).lock();
				}
				System.out.println("Acquired read write lock");
			}
		} else {
			// No lock exists on this page, so acquire one and lock.
			exclusiveLocks.put(pid, new ReentrantLock());
			exclusiveLocks.get(pid).lock();
			if (!pageLockTransactions.contains(pid)) {
				pageLockTransactions.put(pid, new ArrayList<TransactionId>());
			}
			pageLockTransactions.get(pid).add(tid);
		}
    }
    
    public Page getPageSynchronized(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
    	Page p;
    	if (pages.containsKey(pid)) {
    		// place at end of lruCache array
    		synchronized (lruCache) {
    			lruCache.remove(pid);
        		lruCache.add(pid);
        		p = pages.get(pid);
    		}
    	} else {
    		try {
        		// fetch the database file--throws NoSuchElementException if the file is not there
        		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        		// fetch the page from the database file--throws IllegalArgumentException if the pid does not exist.
        		p = file.readPage(pid);
        		if (lruCache.size() == pageTotal) {
        			evictPage();
        		}
        		pages.put(pid, p);
        		lruCache.add(pid);
        	} catch (NoSuchElementException e) {
        		throw new DbException("No database file found.");
        	} catch (IllegalArgumentException e) {
        		throw new TransactionAbortedException();
        	}
    	}
    	return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
    	synchronized (pageLockTransactions) {
    		if (holdsLock(tid, pid)) {
        		if (exclusiveLocks.containsKey(pid)) {
//        			System.out.println(exclusiveLocks.get(pid).toString());
        			exclusiveLocks.remove(pid).unlock();
        		} else if (sharedLocks.containsKey(pid)) {
        			int index = pageLockTransactions.get(pid).indexOf(tid);
//        			System.out.println(sharedLocks.get(pid).toString());
        			if (index >= 0 && sharedLocks.get(pid).size() > index) {
        				sharedLocks.get(pid).remove(index).unlock();
        			}
        		}
        		pageLockTransactions.get(pid).remove(tid);
        		if (pageLockTransactions.get(pid).size() == 0) {
        			pageLockTransactions.remove(pid);
        		}
        	}
    	}
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
    	synchronized (pageLockTransactions) {
    		for (PageId p : pageLockTransactions.keySet()) {
            	if (pageLockTransactions.get(p).contains(tid)) {
            		this.releasePage(tid, p);
            	}
            }
    	}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
    	return pageLockTransactions.containsKey(p) && pageLockTransactions.get(p).contains(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) {
        	this.flushPages(tid);
        }
        this.transactionComplete(tid);
        for (PageId p : this.pages.keySet()) {
        	if (this.pages.get(p).isDirty() == tid) {
        		this.discardPage(p);
        	}
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> dirtypages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	for (int i=0; i<dirtypages.size(); i++) {
    		Page p = dirtypages.get(i);
    		PageId pId = p.getId();
    		if (!this.pages.containsKey(pId)) {
    			this.pages.put(p.getId(), p);	
    		} else {
    			this.lruCache.remove(p.getId());
    		}
    		this.lruCache.add(p.getId());
    		this.pages.get(pId).markDirty(true, tid);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> dirtypages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    	for (int i=0; i<dirtypages.size(); i++) {
    		Page p = dirtypages.get(i);
    		PageId pId = p.getId();
    		if (!this.pages.containsKey(pId)) {
    			this.pages.put(p.getId(), p);
    		} else {
    			this.lruCache.remove(p.getId());
    		}
    		this.lruCache.add(p.getId());
    		this.pages.get(pId).markDirty(true, tid);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (Page page : pages.values()) {
    		if(page.isDirty() != null){
    			flushPage(page.getId());
    		}
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
    	lruCache.remove(pid);
    	pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	Page page = pages.get(pid);
    	if (page.isDirty() != null) {
    		file.writePage(page);
        	page.markDirty(false, null);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (int i=0; i<this.lruCache.size(); i++) {
        	PageId pid = this.lruCache.get(i);
        	if (this.pages.containsKey(pid) && this.pages.get(pid).isDirty() != null
        			&& tid.equals(this.pages.get(pid).isDirty())) {
        		this.flushPage(pid);
        	}
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	PageId pId = this.lruCache.get(0);
    	int index = 1;
    	while (this.pages.get(pId).isDirty() != null && index < this.lruCache.size()) {
    		pId = this.lruCache.get(index);
    		index++;
    	}
    	if (this.pages.get(pId).isDirty() == null) {
    		try {
        		this.flushPage(pId);
        	} catch (IOException e) { 
        		e.printStackTrace();
        	}
        	pages.remove(pId);
        	lruCache.remove(pId);
    	} else {
    		throw new DbException("Can't find page to evict.");
    	}
    	
    }
}
