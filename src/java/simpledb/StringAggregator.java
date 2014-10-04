package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aop;
    private HashMap<Object,Integer> aggregate;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.aop = what;
    	this.aggregate = new HashMap<Object,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Object key = getKey(tup.getField(this.gbfield));
    	if (this.aop == Op.COUNT) {
    		if (this.aggregate.get(key) == null) {
    			this.aggregate.put(key, 1);
    		} else {
    			this.aggregate.put(key, this.aggregate.get(key)+1);
    		}
    	}
    }
    
    public Object getKey(Field f) {
    	if (this.gbfield == NO_GROUPING) {
    		if (!this.aggregate.keySet().contains(NO_GROUPING)) {
    			this.aggregate.put(NO_GROUPING, null);
    		}
    		return NO_GROUPING;
    	} if (!this.aggregate.keySet().contains(f)) {
    		this.aggregate.put(f, null);
    	}
    	return f;
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	Type[] types = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
    	TupleDesc desc = new TupleDesc(types);
    	ArrayList<Tuple> results = new ArrayList<Tuple>();
    	for (Object key : this.aggregate.keySet()) {
    		Tuple newTuple = new Tuple(desc);
    		newTuple.setField(0, (Field)key);
    		newTuple.setField(1, new IntField(this.aggregate.get(key)));
    		results.add(newTuple);
    	}
    	return new TupleIterator(desc, results);
    }
}
