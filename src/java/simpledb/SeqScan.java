package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    /** The transaction this scan is running as a part of */
    private TransactionId transID;
    
    /** The ID of the table scanned */
    private int tableID;
    
    /** The alias of the table being scanned */
    private String alias;
    
    /** An iterator over the tuples in the table being scanned */
    private DbFileIterator fileIterator;
    
    private DbFile dbFile;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.transID = tid;
    	this.tableID = tableid;
    	this.alias = tableAlias;
    	this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableID);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias() {
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableID = tableid;
        this.alias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.fileIterator = Database.getCatalog().getDatabaseFile(this.tableID).iterator(this.transID);
    	this.fileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc currDesc = Database.getCatalog().getDatabaseFile(this.tableID).getTupleDesc();
    	int len = currDesc.numFields();
    	Type[] types = new Type[len];
    	String[] names = new String[len];
    	// iterate through all elements in this tuple description and
    	// edit the names to include the alias of this table
    	for (int i=0; i<len; i++) {
    		types[i] = currDesc.getFieldType(i);
    		names[i] = this.alias + "." + currDesc.getFieldName(i);
    	}
    	// form a new tuple description with the updated information
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.fileIterator != null && this.fileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	if (!this.hasNext()) throw new NoSuchElementException();
        return this.fileIterator.next();
    }

    public void close() {
    	if (this.fileIterator != null) {
    		this.fileIterator.close();
        	this.fileIterator = null;
    	}
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	this.fileIterator.rewind();
    }
    
    public DbFileIterator getResetFileIterator() {
    	return Database.getCatalog().getDatabaseFile(this.tableID).iterator(this.transID);
    }
}
