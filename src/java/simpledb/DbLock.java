package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class handles all of the locks on pages of the database.
 * We can obtain READ_WRITE and READ_ONLY locks on pages.
 */
public class DbLock {
	private ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    
    public DbLock() {
    	sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    	exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
    }
    
    /**
     * Called whenever a transaction tries to acquire an exclusive or shared lock on a page.
     * @param tid The transaction attempting to acquire a page lock.
     * @param pid The page that the transaction is attempting to acquire a lock on.
     * @param perm The permissions being requested--either READ_ONLY or READ_WRITE.
     * @throws TransactionAbortedException If the page cannot be obtained.
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	if (perm == Permissions.READ_ONLY) {
    		handleReadOnlyCase(tid, pid);
    	} else if (perm == Permissions.READ_WRITE) {
    		handleReadWriteCase(tid, pid);
    	}
    }
    
    /**
     * Attempts to get a READ_ONLY lock on a page for a transaction.
     * @param tid The transaction that wishes to acquire the lock.
     * @param pid The page on which the lock should be acquired.
     * @throws TransactionAbortedException If a READ_ONLY lock cannot be obtained.
     */
    public void handleReadOnlyCase(TransactionId tid, PageId pid) throws TransactionAbortedException {
    	long currTime = System.currentTimeMillis();
    	while (true) {
    		// timeout after 200 ms
    		if (200 < (System.currentTimeMillis() - currTime)) {
    			throw new TransactionAbortedException();
    		}
    		synchronized(this) {
    			// Check if there is already a lock on this page
    			if ( (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) ||
    					(sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid))) {
    				break;
    			}
    			// Check if we can acquire a new shared lock on this page
    			if (!exclusiveLocks.containsKey(pid) || exclusiveLocks.get(pid).equals(tid)) {
    				sharedLocks.putIfAbsent(pid, new ArrayList<TransactionId>());
    				if (!sharedLocks.get(pid).contains(tid)) {
    					sharedLocks.get(pid).add(tid);
    				}
    				break;
    			}
    		}
    	}
    }
    
    /**
     * Attempts to get a READ_WRITE lock on a page for a transaction.
     * @param tid The transaction that wishes to acquire the lock.
     * @param pid The page on which the lock should be acquired.
     * @throws TransactionAbortedException If a READ_WRITE lock cannot be obtained.
     */
    public void handleReadWriteCase(TransactionId tid, PageId pid) throws TransactionAbortedException {
    	long currTime = System.currentTimeMillis();
    	while (true) {
    		// timeout
    		if (200 < (System.currentTimeMillis() - currTime)) {
    			throw new TransactionAbortedException();
    		}
    		synchronized(this) {
    			// Check whether an exclusive lock already exists on the page
        		if (exclusiveLocks.containsKey(pid)) {
        			if (exclusiveLocks.get(pid).equals(tid)) {
        				break;
        			} else { 
        				// Cant acquire lock if another transaction holds an exclusive lock on the pae
        				continue;
        			}
        		}
        		// Check whether a shared lock already exists on the page
        		if (!sharedLocks.containsKey(pid) ||
        				(sharedLocks.get(pid).size() == 1 && sharedLocks.get(pid).contains(tid))) {
        			exclusiveLocks.put(pid, tid);
        			break;
        		}
        	}
    	}
    }
    
    /**
     * Releases a page on which there is a lock.
     * @param tid The transaction releasing the page.
     * @param pid The page being released.
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
    	if (exclusiveLocks.containsKey(pid)) {
    		exclusiveLocks.remove(pid);
    	}
    	if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
    		sharedLocks.get(pid).remove(tid);
    		if (sharedLocks.get(pid).size() == 0) {
    			sharedLocks.remove(pid);
    		}
    	}
    }
    
    /**
     * Releases all locks associated with a particular transaction.
     * @param tid The transaction releasing the locks.
     */
    public synchronized void transactionComplete(TransactionId tid) {
    	for (PageId pid : exclusiveLocks.keySet()) {
    		if (exclusiveLocks.get(pid).equals(tid)) {
    			exclusiveLocks.remove(pid);
    		}
    	}
    	for (PageId pid : sharedLocks.keySet()) {
    		if (sharedLocks.get(pid).contains(tid)) {
    			sharedLocks.get(pid).remove(tid);
    		}
    		if (sharedLocks.get(pid).size() == 0) {
    			sharedLocks.remove(pid);
    		}
    	}
    }
    
    /**
     * Checks whether a particular transaction holds a lock on a page.
     * @param tid The transaction being checked.
     * @param pid The page being checked.
     * @return Whether the transaction tid holds a lock on page pid.
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    	if ((exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) ||
    			(sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid))) {
    		return true;
    	}
    	return false;
    }
}
