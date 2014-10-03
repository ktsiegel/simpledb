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
        return new IntegerAggregatorIterator();
    }
    
    public class IntegerAggregatorIterator implements DbIterator {
    	
    	private static final long serialVersionUID = 1L;
		private boolean open;
    	private ArrayList<Object> keys;
    	int index = 0;
    	
    	public IntegerAggregatorIterator() {
    		this.open = false;
    		this.keys = new ArrayList<Object>();
    		this.keys.addAll(IntegerAggregator.this.aggregate.keySet());
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.open = true;
			this.index = 0;
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			if (!this.open || this.index >= keys.size()) return false;
			return true;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			if (this.hasNext()) {
				Object field = this.keys.get(this.index);
				Tuple ntuple = new Tuple(this.getTupleDesc());
				Integer val = (Integer)IntegerAggregator.this.aggregate.get(field);
				if (IntegerAggregator.this.aop == Op.AVG) {
					val /= IntegerAggregator.this.counts.get(field);
				}
				if (field instanceof Field) {
					ntuple.setField(0,(Field)field);
					ntuple.setField(1, new IntField(val));
				} else {
					ntuple.setField(0, new IntField(val));
				}
				this.index++;
				return ntuple;
			}
			return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			this.index = 0;
		}

		@Override
		public TupleDesc getTupleDesc() {
			Type[] types;
			if (IntegerAggregator.this.gbfield == Aggregator.NO_GROUPING) {
				types = new Type[]{Type.INT_TYPE};
			} else {
				types = new Type[]{IntegerAggregator.this.gbfieldtype,Type.INT_TYPE};
			}
			return new TupleDesc(types);
		}

		@Override
		public void close() {
			this.open = false;
		}
    }
}
