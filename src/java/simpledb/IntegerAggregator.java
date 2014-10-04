package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aop;
    private HashMap<Object,Integer> aggregate;
    private HashMap<Object,Integer> counts;
    private int count;
    private boolean hasGField;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.aop = what;
    	this.aggregate = new HashMap<Object,Integer>();
    	this.counts = new HashMap<Object,Integer>();
    	this.hasGField = (this.gbfield != Aggregator.NO_GROUPING);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Object key = getKey(tup);
    	Integer val = ((IntField)tup.getField(this.afield)).getValue();
    	if (this.aggregate.get(key) == null) {
    		if (this.aop == Op.AVG) {
    			this.counts.put(key,1);
    		}
    		if (this.aop == Op.COUNT) {
    			this.aggregate.put(key, 1);
    		} else {
    			this.aggregate.put(key,val);
    		}
    	} else {
    		Integer curVal = this.aggregate.get(key);
    		switch(this.aop) {
        	case MIN: {
        		this.aggregate.put(key, Math.min(val,curVal));
        		break;
        	} case MAX: {
        		this.aggregate.put(key, Math.max(val, curVal));
        		break;
        	} case SUM: {
        		this.aggregate.put(key, val+curVal);
        		break;
        	} case AVG: {
        		int prevCount = this.counts.get(key);
        		this.aggregate.put(key, val + curVal);
        		this.counts.put(key, prevCount+1);
        		break;
        	} case COUNT: {
        		this.aggregate.put(key, curVal+1);
        		break;
        	} default:
    			break;
        	}
    	}
    }
    
    public Object getKey(Tuple tuple) {
    	if (this.gbfield == Aggregator.NO_GROUPING) return null;
    	Field field = tuple.getField(this.gbfield);

    	if (this.gbfield == NO_GROUPING) {
    		if (!this.aggregate.keySet().contains(NO_GROUPING)) {
    			this.aggregate.put(NO_GROUPING, null);
    		}
    		return NO_GROUPING;
    	} if (!this.aggregate.keySet().contains(field)) {
    		this.aggregate.put(field, null);
    	}
    	return field;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {   
    	Type[] types = this.hasGField ? new Type[]{this.gbfieldtype,Type.INT_TYPE} : new Type[]{Type.INT_TYPE};
    	TupleDesc desc = new TupleDesc(types);
    	ArrayList<Tuple> results = new ArrayList<Tuple>();
    	for (Object key: this.aggregate.keySet()) {
    		int value = this.aggregate.get(key);
    		Tuple ntup = new Tuple(desc);
    		Field groupBy = this.hasGField ? (Field)key : new IntField(0);
    		if (this.aop == Aggregator.Op.AVG) {
    			value = value / this.counts.get(key);
    		}
    		Field aggregate = new IntField(value);
    		if (this.hasGField) {
    			ntup.setField(0, groupBy);
    			ntup.setField(1, aggregate);
    		} else {
    			ntup.setField(0, aggregate);
    		}
    		results.add(ntup);
    	}
    	return new TupleIterator(desc, results);
    	
    }
}
