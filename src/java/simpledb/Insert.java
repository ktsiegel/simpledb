package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transId;
    private DbIterator child;
    private int tableid;
    private boolean called;
    private TupleDesc resultTupleDesc;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
    	this.transId = t;
    	this.child = child;
    	this.tableid = tableid;	
    	this.called = false;
    	this.resultTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return this.resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
    	this.child.open();
    	this.called = false;
    	super.open();
    }

    public void close() {
    	this.child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.called) {
        	return null;
        } 
        int count = 0;
        while (this.child.hasNext()) {
        	Tuple next = this.child.next();
        	System.out.println("next: " + next);
        	count++;
        	try {
				Database.getBufferPool().insertTuple(this.transId, this.tableid, next);
			} catch (IOException e) {
				throw new DbException("Could not insert tuple into db");
			}
        }
        Tuple result = new Tuple(this.resultTupleDesc);
        result.setField(0, new IntField(count));
        this.called = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (children.length > 0) {
    		child = children[0];
    	}
    }
}
