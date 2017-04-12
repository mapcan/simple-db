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

    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tid = pid.getTableId();
        int pno = pid.pageNumber();
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] data = HeapPage.createEmptyPageData();

        try {
            FileInputStream in = new FileInputStream(file);
            in.skip(pno * pageSize);
            in.read(data);
            return new HeapPage(new HeapPageId(tid, pno), data);
                 // IllegalArgumentException
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("FileNotFound: " + e);
        } catch (IOException e) {
            throw new IllegalArgumentException("IOError: " + e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        int pn = pid.pageNumber();
        byte[] pageData = page.getPageData();
        RandomAccessFile f = new RandomAccessFile(file, "rws");
        int pageSize = Database.getBufferPool().getPageSize();
        f.skipBytes(pn * pageSize);
        f.write(pageData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)file.length() / Database.getBufferPool().getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            try {
                p.insertTuple(t);
                modifiedPages.add(p);
                return modifiedPages;
            } catch (DbException e) {
                if (e.getMessage() == "page is full") {
                    continue;
                }
                throw new DbException("Tuple cannot be added: " + e);
            }
        }
        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage p = new HeapPage(pid, HeapPage.createEmptyPageData());
        p.insertTuple(t);
        modifiedPages.add(p);
        try {
            writePage(p);
        } catch (IOException e) {
            throw new IOException("File can't be read/written: " + e);
        }
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId)rid.getPageId();
        if (pid.getTableId() != getId()) {
            throw new DbException("Not a member of the file");
        }
        int pn = pid.pageNumber();
        HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        p.deleteTuple(t);
        return p;
    }

    private class HeapFileTupleIterator implements DbFileIterator {
        private int tableId;
        private int curPageNo;
        private TransactionId transactionId;
        private Iterator<Tuple> tupleIterator;

        public HeapFileTupleIterator(TransactionId tid) {
            transactionId = tid;
            tupleIterator = null;
            curPageNo = -1;
            tableId = 0;
        }

        public void open() throws DbException, TransactionAbortedException {
            tableId = getId();
            curPageNo = 0;
            HeapPageId pageId = new HeapPageId(tableId, curPageNo);
            HeapPage page = (HeapPage)Database.getBufferPool().
                getPage(transactionId, pageId, Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }
            boolean hn = tupleIterator.hasNext();
            if (hn) {
                return true;
            }
            if (++curPageNo >= numPages()) {
                return false;
            }
            HeapPageId pageId = new HeapPageId(tableId, curPageNo);
            HeapPage page = (HeapPage)Database.getBufferPool().
                getPage(transactionId, pageId, Permissions.READ_ONLY);
            tupleIterator = page.iterator();
            return tupleIterator.hasNext();
        }

        public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException{
            if (tupleIterator == null || !(tupleIterator.hasNext())) {
                throw new NoSuchElementException("no more tuple");
            }
            return tupleIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            tupleIterator = null;
            curPageNo = -1;
            tableId = 0;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileTupleIterator(tid);
    }
}

