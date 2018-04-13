package edu.buffalo.www.cse4562.operator.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 *
 * @author luthrak
 *
 */
public class HashEquiJoin extends BaseJoin implements Iterator<Object[]>{
	final int block_size = 15;
	private Iterator<Object[]> leftIterator;
	private Iterator<Object[]> rightIterator;
	int leftTupleLength;
	int rightTupleLength;
	int leftJoinColIndex;
	int rightJoinColIndex;
	Object[] resultRow;
	private boolean isHashReady;
	private HashMap<Object, LinkedList<Object[]>> joinerMap;
	private LinkedList<Object[]> bufferedResult;
	private ListIterator<Object[]> bufferedResultIterator;

	public HashEquiJoin(Iterator<Object[]> leftIterator, Iterator<Object[]> rightIterator,
			int leftTupleLength, int rightTupleLength, int leftJoinColIndex, int rightJoinColIndex) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.leftTupleLength = leftTupleLength;
		this.rightTupleLength = rightTupleLength;
		this.leftJoinColIndex = leftJoinColIndex;
		this.rightJoinColIndex = rightJoinColIndex;
		this.resultRow = new Object[leftTupleLength + rightTupleLength];
		this.isHashReady = false;
		this.bufferedResult = new LinkedList<Object[]>();
		this.bufferedResultIterator = this.bufferedResult.listIterator();
	}

	private void copyRowBuffers(Object[] resultRow, Object[] leftRow, Object[] rightRow, int leftLen, int rightLen) {
		for(int i = 0; i < leftLen; i++) {
			resultRow[i] = leftRow[i];
		}
		for(int i = 0; i < rightLen; i++) {
			resultRow[leftLen + i] = rightRow[i];
		}
	}

	private void populateHashMap() {
		this.joinerMap = new HashMap<Object, LinkedList<Object[]>>();
		// read everything from rightIterator and prepare the HashMap
		while(this.rightIterator.hasNext()) {
			Object[] rightRow = this.rightIterator.next();
			LinkedList<Object[]> rowsAtKey = this.joinerMap.get(rightRow[this.rightJoinColIndex]);
			if (rowsAtKey == null) {
				rowsAtKey = new LinkedList<Object[]>();
				rowsAtKey.add(rightRow.clone());
				this.joinerMap.put(rightRow[this.rightJoinColIndex], rowsAtKey);
			} else {
				rowsAtKey.add(rightRow.clone());
			}
		}
	}

	@Override
	public boolean hasNext() {
		if (!this.isHashReady) {
			// read everything from rightIterator and prepare the HashMap
			this.populateHashMap();
			this.isHashReady = true;
			return this.hasNext();
		} else {

			// do not read more rows from left yet
			// there are rows that are prepared from a previous one-to-many join
			if(this.bufferedResultIterator.hasNext()) {
				this.resultRow = this.bufferedResultIterator.next();
				return true;
			}

			// try read another row from left
			while(this.leftIterator.hasNext()) {
				Object[] leftRow = leftIterator.next();
				LinkedList<Object[]> rightRowsAtKey = this.joinerMap.get(leftRow[this.leftJoinColIndex]);
				if(rightRowsAtKey != null) {
					// found a match for this row in the map
					// create result rows now
					this.bufferedResult = new LinkedList<Object[]>();
					for (Object[] rightRow: rightRowsAtKey) {
						Object[] newResultRow = new Object[this.leftTupleLength + this.rightTupleLength];
						copyRowBuffers(newResultRow, leftRow, rightRow, this.leftTupleLength, this.rightTupleLength);
						this.bufferedResult.add(newResultRow);
					}
					this.resultRow = this.bufferedResult.removeFirst();
					this.bufferedResultIterator = this.bufferedResult.listIterator();
					return true;
				}
			}
			return false;
		}
	}

	@Override
	public Object[] next() {
		return resultRow;
	}

	public static void main(String[] main) {
		ArrayList<Object[]> data = new ArrayList<Object[]>();
		data.add(new Object[]{1, "Karan", 25, "Luthra", 562});
		data.add(new Object[]{1, "Karan", 25, "Luthra", 521});
		data.add(new Object[]{2, "Kohli", 24, "Aditya", 562});
		data.add(new Object[]{2, "Kohli", 24, "Aditya", 574});
		data.add(new Object[]{3, "Paritosh", 24, "Walvekar", 562});
		data.add(new Object[]{3, "Paritosh", 24, "Walvekar", 362});

		ArrayList<Object[]> data2 = new ArrayList<Object[]>();
		data2.add(new Object[]{1, "DB", 562});
		data2.add(new Object[]{2, "ML", 574});
		data2.add(new Object[]{3, "OS", 521});
		data2.add(new Object[]{3, "OS", 521});
		data2.add(new Object[]{4, "ALGO", 531});

		/*
		 * Expected input:
		 * both iterators, both tuple sizes, both joining col index in row
		 * Expected output:
		 * 1 Karan 25 Luthra 562 1 DB 562
		 * 1 Karan 25 Luthra 521 3 OS 521
		 * 2 Kohli 24 Aditya 562 1 DB 562
		 * 2 Kohli 24 Aditya 574 2 ML 574
		 */

		HashEquiJoin joiner = new HashEquiJoin(data.iterator(), data2.iterator(), 5, 3, 4, 2);
		//		HashEquiJoin joiner = new HashEquiJoin(data2.iterator(), data.iterator(), 3, 5, 2, 4);

		while(joiner.hasNext()) {
			System.out.println(Arrays.toString(joiner.next()));
		}
	}
}
