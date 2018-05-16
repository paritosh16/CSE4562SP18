package edu.buffalo.www.cse4562.operator.join;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.RecordLocation;
import edu.buffalo.www.cse4562.TableSchema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class IndexJoin extends BaseJoin {
	private Iterator<Object[]> leftIterator;
	private TableSchema rightSchemaObj;
	int leftTupleLength;
	int rightTupleLength;
	int leftJoinColIndex;
	int rightJoinColIndex;
	Object[] resultRow;
	HashMap<Object, RecordLocation> pkindex;
	HashMap<Object, List<RecordLocation>> fkindex;
	RandomAccessFile file;
	int currentRecordIndex;

	public IndexJoin(Iterator<Object[]> leftIterator, TableSchema rightSchemaObj,
			int leftTupleLength, int rightTupleLength, int leftJoinColIndex, int rightJoinColIndex) {
		this.leftIterator = leftIterator;
		this.rightSchemaObj = rightSchemaObj;
		this.leftTupleLength = leftTupleLength;
		this.rightTupleLength = rightTupleLength;
		this.leftJoinColIndex = leftJoinColIndex;
		this.rightJoinColIndex = rightJoinColIndex;
		this.resultRow = new Object[leftTupleLength + rightTupleLength];
		this.currentRecordIndex = 0;

		int indexHint = rightSchemaObj.checkIndex(rightJoinColIndex);
		if (indexHint == 0) {
			pkindex = rightSchemaObj.getPKIndexMap();
		} else if (indexHint == 1) {
			fkindex = rightSchemaObj.getFKIndexMap();
		}

		String filePath = "./data/" + rightSchemaObj.getTableName() + ".dat";
		try {
			file = new RandomAccessFile(filePath, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	private Object[] readIndex(Object indexValue) {
		if (pkindex != null) {
			RecordLocation loc = pkindex.get(indexValue);
			return readFromFile(loc.offset);
		} else {
			List<RecordLocation> locs = fkindex.get(indexValue);
			RecordLocation loc = locs.get(currentRecordIndex);
			currentRecordIndex += 1;
			if (currentRecordIndex == locs.size()) {
				currentRecordIndex = 0;
			}
			return readFromFile(loc.offset);
		}
	}

	private Object[] readFromFile(long position) {

		Object[] record = new Object[this.rightSchemaObj.getTabColumns().size()];
		String line = null;

		try {
			file.seek(position);
			line = file.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (line != null) {
			String[] tempRecord = line.split("\\|");
			for (int i = 0; i < this.rightSchemaObj.getTabColumns().size(); i++) {
				ColumnDefinition tempColumn = this.rightSchemaObj.getTabColumns().get(i);

				String columnType = tempColumn.getColDataType().toString().split(" ")[0];
				if (columnType.toLowerCase().equals("int")) {
					record[i] = new LongValue(tempRecord[i]);
				} else if(columnType.toLowerCase().equals("integer")) {
					record[i] = new LongValue(tempRecord[i]);
				} else if(columnType.toLowerCase().equals("double")) {
					record[i] = new DoubleValue(tempRecord[i]);
				}else if (columnType.toLowerCase().equals("char")) {
					record[i] = new StringValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("varchar")) {
					record[i] = new StringValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("string")) {
					record[i] = new StringValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("decimal")) {
					record[i] = new DoubleValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("date")) {
					record[i] = new DateValue(tempRecord[i]);
				} else {
					System.out.println("Unsupported Data type");
				}
			}
		}
		return record;
	}

	private void copyRowBuffers(Object[] resultRow, Object[] leftRow, Object[] rightRow, int leftLen, int rightLen) {
		for(int i = 0; i < leftLen; i++) {
			resultRow[i] = leftRow[i];
		}
		for(int i = 0; i < rightLen; i++) {
			resultRow[leftLen + i] = rightRow[i];
		}
	}

	@Override
	public boolean hasNext() {
		while(this.leftIterator.hasNext()) {
			Object[] leftRow = leftIterator.next();
			Object[] rightRow = readIndex(leftRow[leftJoinColIndex]);
			this.resultRow = new Object[this.leftTupleLength + this.rightTupleLength];
			copyRowBuffers(this.resultRow, leftRow, rightRow, this.leftTupleLength, this.rightTupleLength);
			return true;
		}

		try {
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Object[] next() {
		return resultRow;
	}
}
