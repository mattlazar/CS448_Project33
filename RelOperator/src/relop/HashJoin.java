package relop;

import index.HashIndex;
public class HashJoin extends Iterator {

	private IndexScan outer;
	private IndexScan inner;
	private int outerPred;
	private int innerPred;

	private boolean startJoin = true;
	
	public HashJoin(Iterator left, Iterator right, int leftPred, int rightPred) {
		//convert and set outer and inner
		if (left instanceof IndexScan) {
			this.outer = (IndexScan) left;
		} else if (left instanceof FileScan) {
			this.outer = new IndexScan(left.getSchema(), new HashIndex(((FileScan) left).getFile().toString()), ((FileScan) left).getFile());
		} else {
			throw new UnsupportedOperationException("not IndexScan or FileScan");
		}

		if (right instanceof IndexScan) {
			this.inner = (IndexScan) right;
		} else if (right instanceof FileScan) {
			this.inner = new IndexScan(right.getSchema(), new HashIndex(((FileScan) right).getFile().toString()), ((FileScan) right).getFile());
		} else {
			throw new UnsupportedOperationException("not IndexScan or FileScan");
		}

		//set everything else
		this.outerPred = leftPred;
		this.innerPred = rightPred;
		this.schema = Schema.join(left.schema, right.schema);
	}

	public void explain(int depth) {
		for (int i = 0; i < depth; i++) System.out.printf(" ");
		System.out.printf("joins two tables using IndexScan");
		outer.explain(depth + 1);
		inner.explain(depth + 1);
	}
	//these methods aren't implemented yet
	public void restart() {
		return;
	}

	public boolean isOpen() {
		return false;
	}

	public void close() {
		return;
	}

	public boolean hasNext() {
		return false;
	}

	public Tuple getNext() {
		return null;
	}
}
