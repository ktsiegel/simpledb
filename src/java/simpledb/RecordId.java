package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /** The page ID referred to by this record ID */
    private PageId page;
    
    /** The tuple number of the record ID on the page */
    private int tupleNumber;
    
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
    	this.page = pid;
    	this.tupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return this.tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.page;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if (!(o instanceof RecordId)) return false;
        RecordId obj = (RecordId) o;
        return obj.page.equals(this.page) && obj.tupleNumber == this.tupleNumber;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return this.page.hashCode() + this.tupleNumber;
    }
}
