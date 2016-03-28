package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  private Iterator iter;
  private Tuple nextTuple;
  private boolean tupleReady;
  private Predicate[] preds;
  
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.iter = iter;
    this.preds = preds;
    this.tupleReady = false;
    this.schema = iter.getSchema();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    for (int i = 0; i < depth; i++) System.out.printf(" ");
    System.out.printf("Selection only returns tuples that satisfy preds");
    iter.explain(depth + 1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if (tupleReady) {
	//we already have something for them
	return true;
    }

    while (iter.hasNext()) {
	nextTuple = iter.getNext();
	boolean passPredTest = false;
	for (Predicate pred : preds) {
		if (pred.evaluate(nextTuple)) passPredTest = true;
	}
	if (passPredTest) {
		tupleReady = true;
		return true;
	}
    }
    return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if (tupleReady) {
    	tupleReady = false;
	return nextTuple;
    } else {
    	if (hasNext()) {
		tupleReady = false;
		return nextTuple;
    	} else {
		throw new IllegalStateException();
	}
    }
  }

} // public class Selection extends Iterator
