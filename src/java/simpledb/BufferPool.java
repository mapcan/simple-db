package simpledb;

import java.io.*;

import java.util.Queue;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

enum LockType {
    SHARED,
    EXCLUSIVE
}

class Lock {
    Object obj;
    LockType type;
    ArrayList<TransactionId> holders;

    public Lock(Object obj, LockType type) {
        this.obj = obj;
        this.type = type;
        this.holders = new ArrayList<TransactionId>();
    }

    public LockType getType() {
        return type;
    }

    public void setType(LockType type) {
        this.type = type;
    }

    public int getHolderSize() {
        return holders.size();
    }

    public void acquire(TransactionId tid) {
        if (type == LockType.SHARED && !holders.contains(tid)) {
            holders.add(tid);
        } else if (type == LockType.EXCLUSIVE && holders.size() <= 1 && !holders.contains(tid)) {
            holders.add(tid);
        }
    }

    public void release(TransactionId tid) {
        holders.remove(tid);
    }

    public boolean isHolder(TransactionId tid) {
        return holders.contains(tid);
    }

    public boolean upgrade(TransactionId tid) {
        if (type != LockType.SHARED) {
            return false;
        }
        if (holders.size() != 1) {
            return false;
        }
        if (holders.get(0) != tid) {
            return false;
        }
        setType(LockType.EXCLUSIVE);
        return true;
    }
}

class LockManager {
    HashMap<Object, Lock> lockTable;
    HashMap<TransactionId, ArrayList<PageId>> transactionTable;

    public LockManager() {
        lockTable = new HashMap<Object, Lock>();
        transactionTable = new HashMap<TransactionId, ArrayList<PageId>>();
    }

    private void updateTransactionTable(TransactionId tid, PageId pid) {
        if (!transactionTable.containsKey(tid)) {
            ArrayList<PageId> pages = new ArrayList<PageId>();
            pages.add(pid);
            transactionTable.put(tid, pages);
        } else {
            ArrayList<PageId> pages = transactionTable.get(tid);
            if (!pages.contains(pid)) {
                pages.add(pid);
            }
        }
    }

    private ArrayList<PageId> getTransactionPages(TransactionId tid) {
        return transactionTable.getOrDefault(tid, new ArrayList<PageId>());
    }

    private void waitFor() {
        try {
            wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void acquireLock(Object obj, TransactionId tid, LockType type) {
        while (true) {
            if (!lockTable.containsKey(obj)) {
                Lock lock = new Lock(obj, type);
                lock.acquire(tid);
                lockTable.put(obj, lock);
                return;
            }
            Lock lock = lockTable.get(obj);
            if (lock.getType() == LockType.SHARED) {
                if (type == LockType.SHARED) {
                    lock.acquire(tid);
                    return;
                } else {
                    if (lock.getHolderSize() == 1 && lock.isHolder(tid)) {
                        lock.upgrade(tid);
                        return;
                    } else {
                        waitFor();
                    }
                }
            } else {
                if (lock.isHolder(tid)) {
                    return;
                }
                waitFor();
            }
        }
    }

    public synchronized void releaseLock(Object obj, TransactionId tid) {
        if (!lockTable.containsKey(obj)) {
            return;
        }
        Lock lock = lockTable.get(obj);
        lock.release(tid);
        if (!(lock.getHolderSize() == 0)) {
            return;
        }
        lockTable.remove(obj);
        notifyAll();
    }

    public synchronized boolean holdsLock(Object obj, TransactionId tid) {
        if (!lockTable.containsKey(obj)) {
            return false;
        }
        Lock lock = lockTable.get(obj);
        return lock.isHolder(tid);
    }
}

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

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    
    //private LinkedList<PageId> pgl;
    private ConcurrentHashMap<PageId, Page> pages;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        //this.pgl = new LinkedList<PageId>();
        this.pages = new ConcurrentHashMap<PageId, Page>();
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return PAGE_SIZE;
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        LockType lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockType.SHARED;
        } else {
            lockType = LockType.EXCLUSIVE;
        }
        lockManager.acquireLock(pid, tid, lockType);
        Page page = pages.get(pid);
        if (page != null) {
            return page;
        }
        page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        while (pages.size() >= numPages) {
            evictPage();
        }
        pages.put(pid, page);
        return page;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p, tid);
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
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
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
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modified = file.insertTuple(tid, t);
        for (Page p : modified) {
            p.markDirty(true, tid);
            pages.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        RecordId rid = t.getRecordId();
        DbFile file = Database.getCatalog().getDatabaseFile(rid.getPageId().getTableId());
        Page modified = file.deleteTuple(tid, t);
        modified.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        if (!pages.containsKey(pid)) {
            return;
        }
        Page page = pages.get(pid);
        if (page.isDirty() == null) {
            return;
        }
        Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        PageId pid = null;
        Enumeration<PageId> e = pages.keys();
        while (e.hasMoreElements()) {
            pid = e.nextElement();
            if (pid == null) {
                continue;
            }
            Page page = pages.get(pid);
            if (page.isDirty() != null) {
                continue;
            }
            discardPage(pid);
            return;
        }
    }

}
