package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DbLock {
	private ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    
    public DbLock() {
    	sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    	exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
    }
    
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
//    	System.out.println("shared locks: " + sharedLocks.toString());
//    	System.out.println("exclusive locks: " + exclusiveLocks.toString());
//    	System.out.println(perm.toString());
    	synchronized (exclusiveLocks) {
    		synchronized (sharedLocks) {
    			if (perm == Permissions.READ_ONLY) {
//            		System.out.println("read only");
            		handleReadOnlyCase(tid, pid, perm);
            	} else if (perm == Permissions.READ_WRITE) {
//            		System.out.println("read write");
            		handleReadWriteCase(tid, pid, perm);
            	}
    		}
    	}
    	
    }
    
    public void handleReadOnlyCase(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	if (exclusiveLocks.containsKey(pid)) {
    		// If it's an exclusive lock, then check if it's within the same transaction.
    		if (!exclusiveLocks.get(pid).equals(tid)) {
    			System.out.println("get read only lock from read write");
    			try {
    				Thread.sleep(100);
    			} catch (InterruptedException e) {
    				System.out.println("oops");
    				throw new TransactionAbortedException();
    			}
        		if (exclusiveLocks.containsKey(pid)) {
        			System.out.println("can't get exclusive lock");
        			throw new TransactionAbortedException();
        		}
    		}
    	} 
    	if (!sharedLocks.containsKey(pid)){ 
    		// No lock on this page, so create a new lock and lock it.
    		sharedLocks.put(pid, new ArrayList<TransactionId>());
    	}
    	sharedLocks.get(pid).add(tid);
    }
    
    public void handleReadWriteCase(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
//    	System.out.println("tid: " + tid + " pid: " + pid);
    	if (!(exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid))) {
    		if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid) && sharedLocks.get(pid).size()==1) {
        		sharedLocks.remove(pid);
        	} else if (exclusiveLocks.containsKey(pid) || sharedLocks.containsKey(pid)) {
        		// If it is already locked exclusively
        		try {
    				Thread.sleep(100);
    			} catch (InterruptedException e) {
    				throw new TransactionAbortedException();
    			}
        		if (exclusiveLocks.containsKey(pid) || sharedLocks.containsKey(pid)) {
        			System.out.println("can't get lock with tid " + tid.toString() + " and pid: " + pid.toString());
        	    	System.out.println("exclusive: " + exclusiveLocks.toString());
        	    	System.out.println("shared: " + sharedLocks.toString());
        			throw new TransactionAbortedException();
        		}
        	}
        	exclusiveLocks.put(pid, tid);
    	}
    }
    
    public void releasePage(TransactionId tid, PageId pid) {
    	synchronized (exclusiveLocks) {
    		if (exclusiveLocks.containsKey(pid)) {
        		exclusiveLocks.remove(pid);
        	}
    	}
		synchronized (sharedLocks) {
			if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
	    		sharedLocks.get(pid).remove(tid);
	    	}
		}
    }
    
    public void transactionComplete(TransactionId tid) {
//    	System.out.println("exclusive before: " + exclusiveLocks.toString());
//    	System.out.println("shared before: " + sharedLocks.toString());
    	synchronized (exclusiveLocks) {
    		for (PageId pid : exclusiveLocks.keySet()) {
        		if (exclusiveLocks.get(pid).equals(tid)) {
        			exclusiveLocks.remove(pid);
        		}
        	}
    	}
    	synchronized (sharedLocks) {
    		for (PageId pid : sharedLocks.keySet()) {
        		if (sharedLocks.get(pid).contains(tid)) {
        			sharedLocks.get(pid).remove(tid);
        		}
        		if (sharedLocks.get(pid).size() == 0) {
        			sharedLocks.remove(pid);
        		}
        	}
    	}
    	
//    	System.out.println("exclusive after: " + exclusiveLocks.toString());
//    	System.out.println("shared after: " + sharedLocks.toString());
    }
    
    public boolean holdsLock(TransactionId tid, PageId pid) {
    	synchronized (exclusiveLocks) {
    		synchronized (sharedLocks) {
    			if ((exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) ||
    	    			(sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid))) {
    	    		return true;
    	    	}
    	    	return false;
    		}
    	}
    }
}
