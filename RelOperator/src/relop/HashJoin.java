package relop;

import index.HashIndex;
import java.util.ArrayList;
import java.util.Arrays;
import global.SearchKey;
import heap.HeapFile;

public class HashJoin extends Iterator {

	private IndexScan outer;
	private IndexScan inner;
	private int outerKey; //the fieldno of the outer that we check for eq
	private int innerKey; //the fieldno of the inner that we check for eq

	private boolean startJoin; //get next tuple in outer

	private boolean tupleFound; //unnecessary, check if nextTuple is empty
	private ArrayList<Tuple>nextTuple; //tuple(s) to return

	private HashTableDup hashTable;
	private int currentBucket; //current bucket of inner (or outer?)
	
	public HashJoin(Iterator left, Iterator right, int leftKey, int rightKey) {
		
		//convert and set outer and inner
		if (left instanceof IndexScan) {
			this.outer = (IndexScan) left;
		} else {
			this.outer = buildHI(left, leftKey);
		}

		if (right instanceof IndexScan) {
			this.inner = (IndexScan) right;
		} else {
			this.inner = buildHI(right, rightKey);
		}

		//set everything else
		this.outerKey = leftKey;
		this.innerKey = rightKey;
		this.schema = Schema.join(left.schema, right.schema);
		this.startJoin = true;
		this.currentBucket = this.inner.getNextHash();
		this.nextTuple = new ArrayList<Tuple>();
		this.hashTable = new HashTableDup();
		newHashTable();
	}

	private IndexScan buildHI(Iterator iter, int keyField) {
		HashIndex tempIndex;
		HeapFile tempHeap;
		Tuple nextTuple;
		if (iter instanceof FileScan) {
			tempIndex = new HashIndex(null);
			tempHeap = ((FileScan) iter).getFile();
			while (iter.hasNext()) {
				nextTuple = iter.getNext();
				tempIndex.insertEntry(new SearchKey(nextTuple.getField(keyField)), ((FileScan) iter).getLastRID());
			}
		} else {
			tempIndex = new HashIndex(null);
			tempHeap = new HeapFile(null);
			while (iter.hasNext()) {
				nextTuple = iter.getNext();
				tempIndex.insertEntry(new SearchKey(nextTuple.getField(keyField)), tempHeap.insertRecord(nextTuple.getData()));
			}
		}
		return new IndexScan(iter.getSchema(), tempIndex, tempHeap);
	}

	public void explain(int depth) {
		for (int i = 0; i < depth; i++) System.out.printf(" ");
		System.out.printf("joins two tables using IndexScan");
		outer.explain(depth + 1);
		inner.explain(depth + 1);
	}
	//these methods aren't implemented yet
	public void restart() {
		outer.restart();
		inner.restart();
	}

	public boolean isOpen() {
		return (outer.isOpen() && inner.isOpen());
	}

	public void close() {
		outer.close();
		inner.close();
	}

	public boolean hasNext() {
		//System.out.println("hasNexting!");
		if (!nextTuple.isEmpty()) {
			//the next tuple is already in nextTuple
			return true;
		}
		
		if (!inner.hasNext()) {
			//scanned everything in inner
			return false;
		}

		if (inner.getNextHash() != currentBucket) {
			//we've moved onto the next partition
			newHashTable();
		}

		Tuple innerTuple;
		while (inner.hasNext()) {
			if (inner.getNextHash() != currentBucket)
				newHashTable(); //this is a new partition
			innerTuple = inner.getNext();
			//System.out.printf("Processing: %s", innerTuple.toString());
			//innerTuple.print();
			//System.out.println();
			Tuple[] fromHT = hashTable.getAll(new SearchKey(innerTuple.getField(innerKey)));
			if (fromHT != null) {
				//make added results to nextTuple
				for (Tuple tuple : fromHT) {
					nextTuple.add(Tuple.join(tuple, innerTuple, this.schema));
				}
				return true;
			}
			
		}
		return false;

	}

	//makes a new hashTable, call whenever currentBucket changes
	private void newHashTable() {
		hashTable.clear();
		currentBucket = inner.getNextHash();
		Tuple outNext;
		while (outer.getNextHash() == currentBucket) {
			outNext = outer.getNext();
			hashTable.add(new SearchKey(outNext.getField(outerKey)), outNext);
			//outNext.print();
		}
		//System.out.printf("Working with hashTable: %s", hashTable.toString());
	}

	public Tuple getNext() {
		//check nextTuple, if empty we call hasNext(), which should fill nextTuple
		if (nextTuple.isEmpty()) {
			//System.out.printf("nextTuple is empty, ");
			if (!hasNext()) {
				throw new IllegalStateException("no more entries");
			}
		}
		//return first tuple in nextTuple and remove it
		Tuple toReturn = nextTuple.get(0);
		nextTuple.remove(0);
		return toReturn;
	}

}
