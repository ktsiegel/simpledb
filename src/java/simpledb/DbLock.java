package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class DbLock {
	private ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    
    public DbLock() {
    	sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    	exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
    }
    
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	if (perm == Permissions.READ_ONLY) {
    		handleReadOnlyCase(tid, pid, perm);
    	} else if (perm == Permissions.READ_WRITE) {
    		handleReadWriteCase(tid, pid, perm);
    	}
    }
    
    public void handleReadOnlyCase(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	long currTime = System.currentTimeMillis();
    	while (true) {
    		if (200 < (System.currentTimeMillis() - currTime)) {
    			throw new TransactionAbortedException();
    		}
    		synchronized(this) {
    			if ( (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) ||
    					(sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid))) {
    				break;
    			}
    			
    			if (!exclusiveLocks.containsKey(pid) || exclusiveLocks.get(pid) == tid) {
    				if (!sharedLocks.containsKey(pid)) {
    					sharedLocks.put(pid, new ArrayList<TransactionId>());
    				}
    				if (!sharedLocks.get(pid).contains(tid)) {
    					sharedLocks.get(pid).add(tid);
    				}
    				break;
    			}
    		}
    	}
    }
    
    public void handleReadWriteCase(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	long currTime = System.currentTimeMillis();
    	
    	while (true) {
    		if (200 < (System.currentTimeMillis() - currTime)) {
    			throw new TransactionAbortedException();
    		}
    		synchronized(this) {
        		if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid) == tid) {
        			break;
        		}
        		if (!sharedLocks.containsKey(pid) ||
        				(sharedLocks.get(pid).size() == 1 && sharedLocks.get(pid).contains(tid))) {
        			exclusiveLocks.put(pid, tid);
        			break;
        		}
        	}
    	}
    }
    
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
    
    public boolean holdsLock(TransactionId tid, PageId pid) {
    	synchronized (this) {
    		if ((exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) ||
    				(sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid))) {
    			return true;
    		}
    		return false;
    	}
    }
}
