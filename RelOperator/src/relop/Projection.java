package relop;



/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  Iterator iter;
  Schema oldSchema;
  Integer[] fields;
  Object[] values;
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    //throw new UnsupportedOperationException("Not implemented");
    this.iter = iter;
    this.oldSchema = iter.getSchema();
    this.fields = fields;
    this.schema = new Schema(fields.length);
    for (int i = 0; i < fields.length; i++) {
    //I'm not sure if Schemas are 0 or 1 indexed, I'll assume 0
	this.schema.initField(i, oldSchema, fields[i]);
    }
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //throw new UnsupportedOperationException("Not implemented");
    for (int i = 0; i < depth; i++) System.out.print(" ");
    System.out.printf("Projection will return a table containing only the specified columns in the specified order\n");
    iter.explain(depth+1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    //throw new UnsupportedOperationException("Not implemented");
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    //throw new UnsupportedOperationException("Not implemented");
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //throw new UnsupportedOperationException("Not implemented");
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //throw new UnsupportedOperationException("Not implemented");
    Tuple raw = iter.getNext();
    Tuple toReturn = new Tuple(this.schema);
    for (int i = 0; i < fields.length; i++) {
    	//again 0 or 1 indexed? assuming 0
	toReturn.setField(i, raw.getField(fields[i]));
    }
    return toReturn;
  }

} // public class Projection extends Iterator
