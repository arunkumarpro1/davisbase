package edu.arun;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

class BTree {

	/**
	 * Filepointer
	 */
	public RandomAccessFile filePointer;

	private long pageSize;

	private boolean isOverflow = false;

	long startOffset = 0, endOffset = 0;;

	static Map<Integer, SerialTypeCode> serialTypeCodesMap;

	static {
		Map<Integer, SerialTypeCode> aMap = new HashMap<>();
		aMap.put(0, new SerialTypeCode(0, 1));
		aMap.put(1, new SerialTypeCode(1, 2));
		aMap.put(2, new SerialTypeCode(2, 4));
		aMap.put(3, new SerialTypeCode(3, 8));
		aMap.put(4, new SerialTypeCode(4, 1));
		aMap.put(5, new SerialTypeCode(5, 2));
		aMap.put(6, new SerialTypeCode(6, 4));
		aMap.put(7, new SerialTypeCode(7, 8));
		aMap.put(8, new SerialTypeCode(8, 4));
		aMap.put(9, new SerialTypeCode(9, 8));
		aMap.put(10, new SerialTypeCode(10, 8));
		aMap.put(11, new SerialTypeCode(11, 8));
		aMap.put(12, new SerialTypeCode(12, 0));
		serialTypeCodesMap = Collections.unmodifiableMap(aMap);
	}

	/**
	 *
	 * @throws IOException
	 */
	public BTree(String tablename, long pageSize) throws IOException {
		filePointer = new RandomAccessFile("data\\" + tablename + ".tbl", "rw");
		this.pageSize = pageSize;
	}

	/**
	 * Insert an entry in the BTree.
	 */
	public int insert(final int key, final Payload payload, final short payloadLength) throws IOException {
		long pageLocation = 0;
		PageHeader pageHeader = getHeader(pageLocation);
		if (pageHeader == null) {
			// BTree is currently empty, create a new root BTreePage
			insertNewLeafPage(payloadLength, key, payload);
		} else {

			while (pageHeader.rightPointer != -1) {
				pageLocation = pageHeader.rightPointer;
				pageHeader = getHeader(pageHeader.rightPointer);
			}

			short dataOffset = (short) (pageHeader.offset - payloadLength - 6);

			// Check for overflow
			if (pageHeader.offset - (payloadLength + 6 + 2) > pageHeader.lengthOfHeader) {
				if (startOffset > 0 && endOffset > 0) {
					filePointer.seek(getOffset(startOffset, endOffset));
					filePointer.writeInt(pageHeader.offset);
					filePointer.writeInt(key);
				}
				filePointer.seek(pageLocation + 8);
				// update header
				if (insertDataOffsetArray(pageHeader.numberOfCells, dataOffset, key)) {
					filePointer.seek(pageLocation + 1);
					filePointer.write(pageHeader.numberOfCells + 1);
					filePointer.writeShort(dataOffset);

					filePointer.seek(dataOffset);
					filePointer.writeShort(payloadLength);
					filePointer.writeInt(key);
					filePointer.write(payload.noOfColumns);
					printSerialTypeCodes(payload.serialTypeCodes);
					for (Column column : payload.columnData)
						printColumnData(column);
				} else {
					createError("ERROR: Already row with the same primary key exists");
					return 0;
				}

			} else {
				// Overflow happpened. Split the page leaf page and modify
				// Internal Page pointers
				split(pageHeader.numberOfCells);
				filePointer.seek(pageLocation + 4);
				filePointer.writeInt((int) filePointer.length());
				long newPageLocation = filePointer.length();
				insertNewLeafPage(payloadLength, key, payload);
				insertNewInternalPage(pageLocation, newPageLocation, key);
			}
		}
		return key;
	}

	// Split the overflow page and insert a new page
	public boolean split(int n) throws IOException {
		Payload payload;
		short payloadLength = 0;
		int key = 0;
		if (isOverflow == true) {
			// Split at the midpoint
			for (int i = 0; i < n / 2; i++)
				filePointer.readShort();
			for (int i = n / 2; i < n; i++) {
				payload = new Payload();
				payload.noOfColumns = (byte) (n - 1);
				payload.columnData = new LinkedList<>();
				payloadLength = filePointer.readShort();
				key = filePointer.readInt();
				payload.serialTypeCodes = readSerialTypeCodes();
				for (byte b : payload.serialTypeCodes)
					payload.columnData.add(readData(b));
				insert(key, payload, payloadLength);
				modifyInternalPointers(filePointer.getFilePointer(), n);
			}
		}
		return true;
	}

	// Method used for modifying the pointers in the internal page
	private void modifyInternalPointers(long point, int numberOfCells) throws IOException {
		int n = 0;
		while (filePointer.readShort() != point) {
			n++;
		}
		while (n <= numberOfCells) {
			filePointer.seek(filePointer.readInt() + 8);
			int nextOffset = filePointer.readShort();
			filePointer.seek(filePointer.readInt() + 8 + ((n - 1) * 2));
			filePointer.writeShort(nextOffset);
			n++;
		}
		modifyInternalPointers(filePointer.getFilePointer(), numberOfCells);
	}

	// Method used for creating a new internal page
	private void insertNewInternalPage(long pageLocation, long newPageLocation, int key) throws IOException {
		startOffset = filePointer.length();
		endOffset = filePointer.length() + pageSize;
		filePointer.setLength(filePointer.length() + pageSize);
		PageHeader header = new PageHeader();
		header.numberOfCells = 1;
		header.pageType = 5;
		header.rightPointer = (int) newPageLocation;
		header.offset = (short) (filePointer.length() - pageLocation);
		header.cellLocations = new short[header.numberOfCells];
		header.cellLocations[0] = header.offset;
		filePointer.seek(filePointer.length() - pageSize);
		filePointer.write(header.pageType);
		filePointer.write(header.numberOfCells);
		filePointer.writeShort(header.offset);
		filePointer.writeInt(header.rightPointer);
		filePointer.writeShort(header.cellLocations[0]);
		filePointer.seek(header.offset);
		filePointer.writeInt((int) pageLocation);
		filePointer.writeInt(key);
	}

	// Method used for creating a new Leaf page
	private void insertNewLeafPage(short payloadLength, int key, Payload payload) throws IOException {
		filePointer.setLength(filePointer.length() + pageSize);
		PageHeader header = new PageHeader();
		header.numberOfCells = 1;
		header.pageType = 13;
		header.rightPointer = -1;
		header.offset = (short) (filePointer.length() - payloadLength - 6);
		header.cellLocations = new short[header.numberOfCells];
		header.cellLocations[0] = header.offset;
		filePointer.seek(filePointer.length() - pageSize);
		filePointer.write(header.pageType);
		filePointer.write(header.numberOfCells);
		filePointer.writeShort(header.offset);
		filePointer.writeInt(header.rightPointer);
		filePointer.writeShort(header.cellLocations[0]);
		filePointer.seek(header.offset);
		filePointer.writeShort(payloadLength);
		filePointer.writeInt(key);
		filePointer.write(payload.noOfColumns);
		printSerialTypeCodes(payload.serialTypeCodes);
		for (Column column : payload.columnData)
			printColumnData(column);
	}

	// Method used for updating the offset array using binary search
	private boolean insertDataOffsetArray(short numberOfCells, short dataOffset, int key) throws IOException {
		long lo = filePointer.getFilePointer();
		long endpoint = lo + (numberOfCells * 2) - 2;
		long hi = endpoint;
		long mid1;
		while (lo <= hi) {
			// Key is in a[lo..hi] or not present.
			long mid = lo + (((mid1 = (hi - lo) / 2) % 2 == 0) ? mid1 : mid1 - 1);
			if (key < getRowIdFromFile(mid))
				hi = mid - 2;
			else if (key > getRowIdFromFile(mid))
				lo = mid + 2;
			else
				return false;
		}
		short offset;
		while (endpoint >= lo) {
			filePointer.seek(endpoint);
			offset = filePointer.readShort();
			filePointer.writeShort(offset);
			endpoint -= 2;
		}
		filePointer.seek(lo);
		filePointer.writeShort(dataOffset);
		return true;
	}

	// Method used for deleting an offset from the offset array
	private void deleteDataOffsetArray(short numberOfCells, short dataOffset, long pageLocation) throws IOException {
		filePointer.seek(pageLocation + 8);
		int n = 1;
		short nextOffset;
		while (filePointer.readShort() != dataOffset) {
			n++;
		}
		while (n <= numberOfCells) {
			filePointer.seek(pageLocation + 8 + (n * 2));
			nextOffset = filePointer.readShort();
			filePointer.seek(pageLocation + 8 + ((n - 1) * 2));
			filePointer.writeShort(nextOffset);
			n++;
		}
	}

	private int getRowIdFromFile(long fp) throws IOException {
		filePointer.seek(fp);
		filePointer.seek(filePointer.readShort() + 2);
		return filePointer.readInt();
	}

	private void printColumnData(Column column) throws NumberFormatException, IOException {
		switch (column.type.toUpperCase()) {
		case "TINYINT":
			filePointer.writeByte(Byte.valueOf(column.column));
			break;
		case "NULL TINYINT":
			filePointer.writeByte(0);
			break;
		case "SMALLINT":
			filePointer.writeShort(Short.valueOf(column.column));
			break;
		case "NULL SMALLINT":
			filePointer.writeShort(0);
			break;
		case "INT":
			filePointer.writeInt(Integer.valueOf(column.column));
			break;
		case "NULL INT":
			filePointer.writeInt(0);
			break;
		case "BIGINT":
			filePointer.writeLong(Long.valueOf(column.column));
			break;
		case "REAL":
			filePointer.writeFloat(Float.valueOf(column.column));
			break;
		case "DOUBLE":
			filePointer.writeDouble(Double.valueOf(column.column));
			break;
		case "NULL DOUBLE":
			filePointer.writeDouble(0);
			break;
		case "DATETIME":
			filePointer.writeLong(convertDateTimeToLong(column.column));
			break;
		case "DATE":
			filePointer.writeLong(convertDateToLong(column.column));
			break;
		case "TEXT":
			filePointer.writeBytes(column.column);
			break;
		}
	}

	private long convertDateToLong(String column) {
		/* Define the time zone for Dallas CST */
		ZoneId zoneId = ZoneId.of("America/Chicago");

		// 9999-12-31 23:59:59
		String dateSplit[] = column.split("-");

		/*
		 * Convert date and time parameters for 1974-05-27 to a ZonedDateTime
		 * object
		 */
		ZonedDateTime zdt = ZonedDateTime.of(Integer.parseInt(dateSplit[0]), Integer.parseInt(dateSplit[1]),
				Integer.parseInt(dateSplit[2]), 0, 0, 0, 0, zoneId);

		// Convert a ZonedDateTime object to epochSeconds
		return zdt.toInstant().toEpochMilli() / 1000;
	}

	private LocalDate convertLongToDate(long date) {
		ZoneId zoneId = ZoneId.of("America/Chicago");
		Instant i = Instant.ofEpochSecond(date);
		ZonedDateTime zdt2 = ZonedDateTime.ofInstant(i, zoneId);
		return zdt2.toLocalDate();
	}

	private long convertDateTimeToLong(String column) {

		/* Define the time zone for Dallas CST */
		ZoneId zoneId = ZoneId.of("America/Chicago");

		// 9999-12-31 23:59:59
		String split[] = column.split(" ");
		String dateSplit[] = split[0].split("-");
		String timeSplit[] = split[1].split(":");

		/*
		 * Convert date and time parameters for 1974-05-27 to a ZonedDateTime
		 * object
		 */
		ZonedDateTime zdt = ZonedDateTime.of(Integer.parseInt(dateSplit[0]), Integer.parseInt(dateSplit[1]),
				Integer.parseInt(dateSplit[2]), Integer.parseInt(timeSplit[0]), Integer.parseInt(timeSplit[1]),
				Integer.parseInt(timeSplit[2]), 0, zoneId);

		// Convert a ZonedDateTime object to epochSeconds
		return zdt.toInstant().toEpochMilli() / 1000;
	}

	private ZonedDateTime convertLongToDateTime(long datetime) {
		ZoneId zoneId = ZoneId.of("America/Chicago");
		Instant i = Instant.ofEpochSecond(datetime);
		ZonedDateTime zdt2 = ZonedDateTime.ofInstant(i, zoneId);
		return zdt2;
	}

	private void printSerialTypeCodes(byte[] serialTypeCodes) throws IOException {
		for (byte b : serialTypeCodes)
			filePointer.write(b);
	}

	/**
	 * Return the root BTreePage, or null if it doesn't exist.
	 */
	PageHeader getHeader(long pos) throws IOException {
		if (filePointer.length() == 0)
			return null;
		filePointer.seek(pos);
		PageHeader header = new PageHeader();
		header.pageType = filePointer.readByte();
		header.numberOfCells = filePointer.readByte();
		header.offset = filePointer.readShort();
		header.rightPointer = filePointer.readInt();
		header.cellLocations = new short[header.numberOfCells];
		for (int i = 0; i < header.numberOfCells; i++)
			header.cellLocations[i] = filePointer.readShort();
		header.lengthOfHeader = (int) filePointer.getFilePointer();
		return header;
	}

	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}

	private long getOffset(long startOffset, long endOffset) {
		return ThreadLocalRandom.current().nextLong(startOffset + 1, endOffset + 1);
	}

	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is = new ObjectInputStream(in);
		return is.readObject();
	}

	// Iterate through the BTree and get the highest rowId.
	public int getLastRowId() throws IOException {
		if (getHeader(0) == null)
			return 0;
		filePointer.seek(4);
		int rightLeafPointer = filePointer.readInt();
		while (rightLeafPointer != -1) {
			filePointer.seek(rightLeafPointer + 4);
			rightLeafPointer = filePointer.readInt();
		}
		filePointer.seek(filePointer.getFilePointer() - 7);
		byte n = filePointer.readByte();
		long value1 = filePointer.getFilePointer() + 6;
		long value = value1 + ((n - 1) * 2);
		filePointer.seek(value);
		filePointer.seek(filePointer.readShort() + 2);
		return filePointer.readInt();
	}

	public List<List<Column>> selectTable(int noOfColumns) throws IOException {
		int pageLocation = 0;
		PageHeader pageHeader = null;
		filePointer.seek(0);
		List<List<Column>> data = new LinkedList<>();
		List<Column> tuple;
		byte[] serialTypeCodes;
		if (filePointer.length() != 0)
			do {
				pageHeader = getHeader(pageLocation);
				for (short offset : pageHeader.cellLocations) {
					tuple = new LinkedList<>();
					filePointer.seek(offset + 2);
					tuple.add(new Column(String.valueOf(filePointer.readInt()), "INT", "N", "Y"));
					serialTypeCodes = readSerialTypeCodes();
					for (byte b : serialTypeCodes)
						tuple.add(readData(b));
					data.add(tuple);
				}
				pageLocation = pageHeader.rightPointer;
			} while (pageHeader.rightPointer != -1);
		return data;
	}

	private Column readData(byte b) throws NumberFormatException, IOException {
		switch (b) {
		case 0:
			filePointer.readByte();
			return new Column("NULL", "NULL TINYINT", "", "N");
		case 1:
			filePointer.readShort();
			return new Column("NULL", "NULL SMALLINT", "", "N");
		case 2:
			filePointer.readInt();
			return new Column("NULL", "NULL INT", "", "N");
		case 3:
			filePointer.readDouble();
			return new Column("NULL", "NULL DOUBLE", "", "N");
		case 4:
			return new Column(String.valueOf(filePointer.readByte()), "TINYINT", "", "N");
		case 5:
			return new Column(String.valueOf(filePointer.readShort()), "SMALLINT", "", "N");
		case 6:
			return new Column(String.valueOf(filePointer.readInt()), "INT", "", "N");
		case 7:
			return new Column(String.valueOf(filePointer.readLong()), "BIGINT", "", "N");
		case 8:
			return new Column(String.valueOf(filePointer.readFloat()), "REAL", "", "N");
		case 9:
			return new Column(String.valueOf(filePointer.readDouble()), "DOUBLE", "", "N");
		case 10:
			return new Column(convertLongToDateTime(filePointer.readLong()).toString(), "DATETIME", "", "N");
		case 11:
			return new Column(convertLongToDate(filePointer.readLong()).toString(), "DATE", "", "N");
		default:
			if (b == 12)
				return new Column("NULL", "TEXT", "", "Y");
			byte[] text = new byte[b - 12];
			filePointer.read(text);
			return new Column(new String(text), "TEXT", "", "N");
		}
	}

	private byte[] readSerialTypeCodes() throws IOException {
		byte n = filePointer.readByte();
		byte[] serialTypeCodes = new byte[n];
		for (int i = 0; i < n; i++)
			serialTypeCodes[i] = filePointer.readByte();
		return serialTypeCodes;
	}

	public List<List<Column>> select(int wherePosition, String whereValue, int noOfColumns, String operator,
			String whereType) throws IOException {
		int pageLocation = 0;
		filePointer.seek(0);
		PageHeader pageHeader = null;
		List<List<Column>> data = new LinkedList<>();
		List<Column> tuple;
		byte[] serialTypeCodes;
		if (filePointer.length() != 0)
			do {
				pageHeader = getHeader(pageLocation);
				for (short offset : pageHeader.cellLocations) {
					tuple = new LinkedList<>();
					filePointer.seek(offset + 2);
					tuple.add(new Column(String.valueOf(filePointer.readInt()), "INT", "N", "Y"));
					serialTypeCodes = readSerialTypeCodes();
					for (byte b : serialTypeCodes)
						tuple.add(readData(b));
					if (isCompareSuccessful(tuple.get(wherePosition - 1).column, whereValue, whereType, operator))
						data.add(tuple);
				}
				pageLocation = pageHeader.rightPointer;
			} while (pageHeader.rightPointer != -1);
		return data;
	}

	public int update(int wherePosition, List<Integer> updatePositions, String whereValue, List<String> updateValues,
			int noOfColumns, String operator, String whereType, String updateType) throws IOException {
		int pageLocation = 0;
		filePointer.seek(0);
		PageHeader pageHeader = null;
		Payload payload;
		int key, i, numOfRows = 0;
		short payloadLength;
		String oldValue = null, updateValue = null;
		do {
			pageHeader = getHeader(pageLocation);
			for (short offset : pageHeader.cellLocations) {
				payload = new Payload();
				payload.noOfColumns = (byte) (noOfColumns - 1);
				payload.columnData = new LinkedList<>();
				filePointer.seek(offset);
				payloadLength = filePointer.readShort();
				key = filePointer.readInt();
				payload.serialTypeCodes = readSerialTypeCodes();
				for (byte b : payload.serialTypeCodes)
					payload.columnData.add(readData(b));
				if (isCompareSuccessfulForUpdate(offset, wherePosition, whereValue, whereType, operator,
						payload.serialTypeCodes)) {
					numOfRows++;
					i = 0;
					for (int updatePosition : updatePositions) {
						oldValue = payload.columnData.get(updatePosition - 2).column;
						updateValue = updateValues.get(i++);
						payloadLength = (short) (payloadLength - oldValue.length() + updateValue.length());
						payload.serialTypeCodes[updatePosition - 2] = (byte) (12 + updateValue.length());
						payload.columnData.get(updatePosition - 2).column = updateValue;
					}
					deleteDataOffsetArray(pageHeader.numberOfCells, offset, pageLocation);
					filePointer.seek(pageLocation + 1);
					filePointer.write(pageHeader.numberOfCells - 1);
					insert(key, payload, payloadLength);
					filePointer.seek(pageLocation + 1);
					pageHeader.numberOfCells = filePointer.readByte();
				}
			}
			pageLocation = pageHeader.rightPointer;
		} while (pageHeader.rightPointer != -1);
		return numOfRows;
	}

	public int delete(int wherePosition, String whereValue, int noOfColumns, String operator, String whereType)
			throws IOException {
		filePointer.seek(0);
		PageHeader pageHeader = getHeader(0);
		long pageLocation = 0;
		int numberOfRows = 0;
		byte[] serialTypeCodes;
		do {
			pageHeader = getHeader(pageLocation);
			for (short offset : pageHeader.cellLocations) {
				filePointer.seek(offset + 6);
				serialTypeCodes = readSerialTypeCodes();
				if (isCompareSuccessfulForUpdate(offset, wherePosition, whereValue, whereType, operator,
						serialTypeCodes)) {
					numberOfRows++;
					deleteDataOffsetArray(pageHeader.numberOfCells, offset, pageLocation);
					filePointer.seek(pageLocation + 1);
					filePointer.write(--pageHeader.numberOfCells);
				}
			}
			pageLocation = pageHeader.rightPointer;
		} while (pageHeader.rightPointer != -1);
		return numberOfRows;
	}

	private boolean isCompareSuccessfulForUpdate(short offset, int wherePosition, String whereValue, String whereType,
			String operator, byte[] serialTypeCodes) throws NumberFormatException, IOException {
		if (wherePosition == 0)
			return true;
		if (wherePosition == 1) {
			filePointer.seek(offset + 2);
			// TO check if it is a number
			if (whereValue.matches("-?\\d+(\\.\\d+)?")) {
				int c = filePointer.readInt();
				if (c == Integer.valueOf(whereValue))
					return true;
			} else {
				createError("Primary Key must be an integer");
			}
		} else
			return isCompareSuccessful(readColumnValue(offset, wherePosition, serialTypeCodes), whereValue, whereType,
					operator);
		return false;
	}

	private String readColumnValue(short offset, int wherePosition, byte[] serialTypeCodes)
			throws NumberFormatException, IOException {
		filePointer.seek(
				offset + 6 + serialTypeCodes.length + 1 + getColumnLocationFromOffset(wherePosition, serialTypeCodes));
		return readData(serialTypeCodes[wherePosition - 2]).column;

	}

	private long getColumnLocationFromOffset(int wherePosition, byte[] serialTypeCodes) {
		long jumpLength = 0;
		for (int i = 2; i < wherePosition; i++) {
			jumpLength += serialTypeCodes[i - 2] > 12 ? serialTypeCodes[i - 2] - 12
					: BTree.serialTypeCodesMap.get(serialTypeCodes[i - 2]).length;
		}
		return jumpLength;
	}

	private boolean isCompareSuccessful(String columnValue, String whereValue, String whereType, String operator) {
		boolean success = false;
		switch (operator) {
		case "=":
			if (whereType.equals("TEXT"))
				success = columnValue.equalsIgnoreCase(whereValue);
			else if (whereType.equals("DATE"))
				success = convertLongToDate(Long.valueOf(columnValue))
						.equals(convertLongToDate(Long.valueOf(whereValue)));
			else if (whereType.equals("DATETIME"))
				success = convertLongToDateTime(Long.valueOf(columnValue))
						.equals(convertLongToDateTime(Long.valueOf(whereValue)));
			else
				success = new BigDecimal(columnValue).equals(new BigDecimal(whereValue));

			break;
		case "!=":
		case "<>":
			if (whereType.equals("TEXT"))
				success = !columnValue.equalsIgnoreCase(whereValue);
			else if (whereType.equals("DATE"))
				success = !(convertLongToDate(Long.valueOf(columnValue))
						.equals(convertLongToDate(Long.valueOf(whereValue))));
			else if (whereType.equals("DATETIME"))
				success = !(convertLongToDateTime(Long.valueOf(columnValue))
						.equals(convertLongToDateTime(Long.valueOf(whereValue))));
			else
				success = !(new BigDecimal(columnValue).equals(new BigDecimal(whereValue)));
			break;

		case ">":
			if (whereType.equals("TEXT"))
				success = columnValue.compareTo(whereValue) == 1 || columnValue.compareTo(whereValue) == 0 ? true
						: false;
			else if (whereType.equals("DATE"))
				success = convertLongToDate(Long.valueOf(columnValue))
						.isAfter(convertLongToDate(Long.valueOf(whereValue)))
						|| convertLongToDate(Long.valueOf(columnValue))
								.equals(convertLongToDate(Long.valueOf(whereValue)));
			else if (whereType.equals("DATETIME"))
				success = convertLongToDateTime(Long.valueOf(columnValue))
						.isAfter(convertLongToDateTime(Long.valueOf(whereValue)))
						|| convertLongToDateTime(Long.valueOf(columnValue))
								.equals(convertLongToDateTime(Long.valueOf(whereValue)));
			else
				success = new BigDecimal(columnValue).compareTo(new BigDecimal(whereValue)) == 1
						|| new BigDecimal(columnValue).equals(new BigDecimal(whereValue)) ? true : false;
			break;

		case "<":
			if (whereType.equals("TEXT"))
				success = columnValue.compareTo(whereValue) == -1 ? true : false;
			else if (whereType.equals("DATE"))
				success = convertLongToDate(Long.valueOf(columnValue))
						.isBefore(convertLongToDate(Long.valueOf(whereValue)))
						|| convertLongToDate(Long.valueOf(columnValue))
								.equals(convertLongToDate(Long.valueOf(whereValue)));
			else if (whereType.equals("DATETIME"))
				success = convertLongToDateTime(Long.valueOf(columnValue))
						.isBefore(convertLongToDateTime(Long.valueOf(whereValue)))
						|| convertLongToDateTime(Long.valueOf(columnValue))
								.equals(convertLongToDateTime(Long.valueOf(whereValue)));
			else
				success = new BigDecimal(columnValue).compareTo(new BigDecimal(whereValue)) == -1
						|| new BigDecimal(columnValue).equals(new BigDecimal(whereValue)) ? true : false;
			break;

		case ">=":
			if (whereType.equals("TEXT"))
				success = columnValue.compareTo(whereValue) == 1 ? true : false;
			else if (whereType.equals("DATE"))
				success = convertLongToDate(Long.valueOf(columnValue))
						.isAfter(convertLongToDate(Long.valueOf(whereValue)));
			else if (whereType.equals("DATETIME"))
				success = convertLongToDateTime(Long.valueOf(columnValue))
						.isAfter(convertLongToDateTime(Long.valueOf(whereValue)));
			else
				success = new BigDecimal(columnValue).compareTo(new BigDecimal(whereValue)) == 1 ? true : false;
			break;

		case "<=":
			if (whereType.equals("TEXT"))
				success = columnValue.compareTo(whereValue) == -1 ? true : false;
			else if (whereType.equals("DATE"))
				success = convertLongToDate(Long.valueOf(columnValue))
						.isBefore(convertLongToDate(Long.valueOf(whereValue)));
			else if (whereType.equals("DATETIME"))
				success = convertLongToDateTime(Long.valueOf(columnValue))
						.isBefore(convertLongToDateTime(Long.valueOf(whereValue)));
			else
				success = new BigDecimal(columnValue).compareTo(new BigDecimal(whereValue)) == -1 ? true : false;
			break;

		default:
			createError("Error in parsing relational operator: " + operator);
		}
		return success;
	}

	// BTree search if where clause has primary key
	public List<List<Column>> selectPrimary(String whereValue, String operator) throws IOException {
		int pageLocation = 0;
		filePointer.seek(0);
		PageHeader pageHeader = null;
		List<List<Column>> data = new LinkedList<>();
		List<Column> tuple;
		byte[] serialTypeCodes;
		if (filePointer.length() != 0)
			do {
				pageHeader = getHeader(pageLocation);
				for (short offset : pageHeader.cellLocations) {
					tuple = new LinkedList<>();
					filePointer.seek(offset + 2);
					tuple.add(new Column(String.valueOf(filePointer.readInt()), "INT", "N", "Y"));
					serialTypeCodes = readSerialTypeCodes();
					for (byte b : serialTypeCodes)
						tuple.add(readData(b));
					if (isCompareSuccessful(tuple.get(0).column, whereValue, "INT", operator))
						data.add(tuple);
				}
				pageLocation = pageHeader.rightPointer;
			} while (pageHeader.rightPointer != -1);
		return data;
	}

	public static void createError(String err) {
		System.out.println("***Error: " + err + "***\n");
	}

}
