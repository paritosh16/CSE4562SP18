package edu.buffalo.www.cse4562.operator.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * @author luthrak
 *
 */
public class BlockNestedLoopJoin implements Iterator<Object[]>{
	final int block_size = 2;
	Iterator<Object[]> leftIterator;
	Iterator<Object[]> rightIterator;
	int leftTupleLength;
	int rightTupleLength;
	ArrayList<Object[]> resultBlock;
	Iterator<Object[]> resultIterator;
	boolean flagOutOfRows;
	boolean flagCacheFilled;
	Object[][] leftBlock;

	ArrayList<Object[]> inMemoryCache;

	public BlockNestedLoopJoin(Iterator<Object[]> leftIterator, Iterator<Object[]> rightIterator, int leftTupleLength, int rightTupleLength) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.leftTupleLength = leftTupleLength;
		this.rightTupleLength = rightTupleLength;
		resultBlock = new ArrayList<Object[]>();
		inMemoryCache = new ArrayList<Object[]>();
		this.resultIterator = this.resultBlock.iterator();
		this.leftBlock = this.readBlock(leftIterator);

		//		System.out.println("New Left Block:");
		//		for (Object[] row: this.leftBlock) {
		//			System.out.println(Arrays.toString(row));
		//		}

		this.flagOutOfRows = false;
		this.flagCacheFilled = false;
	}

	private Object[][] readBlock(Iterator<Object[]> iterator) {
		int i = 0;
		Object[][] localBuffer = new Object[this.block_size][];
		while(iterator.hasNext()) {
			localBuffer[i] = iterator.next().clone();
			if (++i == this.block_size) {
				break;
			}
		}
		if (i == 0) {
			//			System.out.println("-----out of blocks------");
			this.flagOutOfRows = true;
		}
		return localBuffer;
	}

	private Object[][] readBlockTryCache(Iterator<Object[]> iterator) {
		int i = 0;
		Object[][] localBuffer = new Object[this.block_size][];
		while(iterator.hasNext()) {
			localBuffer[i] = iterator.next().clone();
			if (!this.flagCacheFilled) {
				this.inMemoryCache.add(localBuffer[i]);
			}
			if (++i == this.block_size) {
				break;
			}
		}
		if (i == 0) {
			//			System.out.println("-----out of blocks------");
			this.flagOutOfRows = true;
		}
		return localBuffer;
	}


	private boolean computeNextBlocks() {
		assert(!resultIterator.hasNext());
		this.resultBlock.clear();
		// @karan: check this
		this.resultIterator = null;

		Object[] resultRow = new Object[this.leftTupleLength + this.rightTupleLength];

		Object[][] rightBlock = this.readBlockTryCache(this.rightIterator);
		//		System.out.println("New Right Block:");
		//		for (Object[] row: rightBlock) {
		//			System.out.println(Arrays.toString(row));
		//		}
		if(this.flagOutOfRows) {
			// ran out of rows on right iterator
			this.flagOutOfRows = false;
			//						System.out.println("-----out of right blocks------");

			// one full iteration of right side has completed
			// fetch new block on left, and reset right block iterator to beginning
			this.leftBlock = this.readBlock(this.leftIterator);
			//			System.out.println("New Left Block:");
			//			for (Object[] row: this.leftBlock) {
			//				System.out.println(Arrays.toString(row));
			//			}
			if(this.flagOutOfRows) {
				return false;
			} else {
				this.rightIterator = this.inMemoryCache.iterator();
				this.flagCacheFilled = true;
				rightBlock = this.readBlockTryCache(this.rightIterator);
				//				System.out.println("New Right Block:");
				//				for (Object[] row: rightBlock) {
				//					System.out.println(Arrays.toString(row));
				//				}
			}
		}

		for (Object[] leftRow : this.leftBlock) {
			if (leftRow == null) break;

			for (Object[] rightRow : rightBlock) {
				if (rightRow == null) break;

				// copy each cell now
				int j = 0;
				// copy from left
				for (int i = 0; i < leftRow.length; i++) {
					resultRow[j++] = leftRow[i];
				}
				// copy from right
				for (int i = 0; i < rightRow.length; i++) {
					resultRow[j++] = rightRow[i];
				}
				this.resultBlock.add(resultRow.clone());
			}
		}
		// reset iterator
		this.resultIterator = this.resultBlock.iterator();
		return true;
	}

	@Override
	public boolean hasNext() {
		// check if there's more rows in the resultBlock and return True if yes
		if (this.resultIterator.hasNext()) {
			return true;
		}
		// else check if there's more blocks (on either operators) left to process
		// if there are, processes them and then return True if there's more rows in resultBlock now
		else {
			return computeNextBlocks() && this.resultIterator.hasNext();
		}
	}

	@Override
	public Object[] next() {
		return this.resultIterator.next();
	}

	public static void main(String[] main) {
		ArrayList<Object[]> data = new ArrayList<Object[]>();
		data.add(new Object[]{1, "Karan", 25, "Luthra"});
		data.add(new Object[]{2, "Kohli", 24, "Aditya"});
		data.add(new Object[]{3, "Pari", 22, "Walve"});

		ArrayList<Object[]> data2 = new ArrayList<Object[]>();
		data2.add(new Object[]{1, "DB", 562});
		data2.add(new Object[]{2, "ML", 574});
		data2.add(new Object[]{1, "OS", 521});


		BlockNestedLoopJoin joiner = new BlockNestedLoopJoin(data.iterator(), data2.iterator(), 4, 3);
		while(joiner.hasNext()) {
			System.out.println(Arrays.toString(joiner.next()));
		}
	}
}
