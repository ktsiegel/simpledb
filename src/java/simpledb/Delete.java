package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transId;
    private DbIterator child;
    private TupleDesc resultTupleDesc;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	this.transId = t;
    	this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.called) return null;
    	int count = 0;
		while (child.hasNext()) {
			Tuple next = child.next();
			count++;
			try {
				Database.getBufferPool().deleteTuple(transId, next);
			} catch (IOException e) {
				throw new DbException("An error occured");
			}
		}
    	Tuple output = new Tuple(this.resultTupleDesc);
    	IntField intField = new IntField(count);
    	output.setField(0, intField);
        this.called = true;
        return output;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (children.length > 0) {
            this.child = children[0];
    	}
    }

}
