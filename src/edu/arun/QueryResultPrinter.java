package edu.arun;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class QueryResultPrinter {

	private static final int COLUMN_WIDTH = 150;

	public static final int STRING_GROUP = 1;

	public static final int INTEGER_GROUP = 2;

	public static final int DOUBLE_GROUP = 3;

	public static final int DATETIME_GROUP = 4;

	public static final int BOOLEAN_GROUP = 5;

	public static final int OTHER = 0;

	static Map<String, Integer> sqlTypes;

	static {
		Map<String, Integer> aMap = new HashMap<>();
		aMap.put("NULL TINYINT", Types.TINYINT);
		aMap.put("NULL SMALLINT", Types.SMALLINT);
		aMap.put("NULL INT", Types.INTEGER);
		aMap.put("NULL DOUBLE", Types.DOUBLE);
		aMap.put("TINYINT", Types.TINYINT);
		aMap.put("SMALLINT", Types.SMALLINT);
		aMap.put("INT", Types.INTEGER);
		aMap.put("BIGINT", Types.BIGINT);
		aMap.put("REAL", Types.REAL);
		aMap.put("DOUBLE", Types.DOUBLE);
		aMap.put("DATETIME", Types.DATE);
		aMap.put("DATE", Types.DATE);
		aMap.put("TEXT", Types.VARCHAR);
		sqlTypes = Collections.unmodifiableMap(aMap);
	}

	/**
	 * Represents a database table column.
	 */
	private static class Field {

		private String label;

		private String type;

		private String typeName;

		private int width = 0;

		private List<String> values = new ArrayList<>();

		private String justifyFlag = "";

		private int typeCategory = 0;

		public Field(String label, String type, String typeName) {
			this.label = label;
			this.type = type;
			this.typeName = typeName;
		}

		/**
		 * Returns the column label
		 *
		 * @return Column label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * Returns the generic SQL type of the column
		 *
		 * @return Generic SQL type
		 */
		public String getType() {
			return type;
		}

		/**
		 * Returns the generic SQL type name of the column
		 *
		 * @return Generic SQL type name
		 */
		public String getTypeName() {
			return typeName;
		}

		/**
		 * Returns the width of the column
		 *
		 * @return Column width
		 */
		public int getWidth() {
			return width;
		}

		/**
		 * Sets the width of the column to <code>width</code>
		 *
		 * @param width
		 *            Width of the column
		 */
		public void setWidth(int width) {
			this.width = width;
		}

		/**
		 *
		 * @param value
		 *            The column value to add to {@link #values}
		 */
		public void addValue(String value) {
			values.add(value);
		}

		/**
		 * @param i
		 *            The index of the column value to get
		 * @return The String representation of the value
		 */
		public String getValue(int i) {
			return values.get(i);
		}

		/**
		 */
		public String getJustifyFlag() {
			return justifyFlag;
		}

		/**
		 * text will be left justified.
		 */
		public void justifyLeft() {
			this.justifyFlag = "-";
		}

		/**
		 * Returns the generic SQL type category of the column
		 *
		 */
		public int getTypeCategory() {
			return typeCategory;
		}

		/**
		 *
		 * @param typeCategory
		 *            The type category
		 */
		public void setTypeCategory(int typeCategory) {
			this.typeCategory = typeCategory;
		}
	}

	/**
	 * @param ordinalPostions
	 * @param rs
	 * @param maxStringColWidth
	 */
	public static void printQueryResult(List<List<Column>> resultSet, List<List<Column>> columnss, String tableName,
			List<Integer> ordinalPostions) {
		int maxStringColWidth = COLUMN_WIDTH;

		// Total number of columns in this ResultSet
		int columnCount = columnss.size();

		// List of Column objects to store each columns of the ResultSet
		// and the String representation of their values.
		List<Field> fields = new ArrayList<>(columnCount);

		// List of table names. Can be more than one if it is a joined
		// table query
		List<String> tableNames = new ArrayList<>();
		tableNames.add(tableName);

		// Get the columns and their meta data.
		if (columnCount > 1)
			for (int i = 0; i < columnCount; i++) {
				Field c = new Field(columnss.get(i).get(2).column, columnss.get(i).get(3).column,
						columnss.get(i).get(3).column);
				c.setWidth(c.getLabel().length());
				c.setTypeCategory(getGroup(c.getType()));
				fields.add(c);

			}
		else {
			Field c = new Field(columnss.get(0).get(0).column, "TEXT", "TEXT");
			c.setWidth(c.getLabel().length());
			c.setTypeCategory(getGroup(c.getType()));
			fields.add(c);
		}

		// Go through each row, get values of each column and adjust
		// column widths.
		int rowCount = 0;
		for (List<Column> tuple : resultSet) {
			for (int i = 0; i < columnCount; i++) {
				Field c = fields.get(i);
				String value = null;
				int category = c.getTypeCategory();

				if (category == OTHER) {

					// Use generic SQL type name instead of the actual value
					// for column types BLOB, BINARY etc.
					value = "(" + c.getTypeName() + ")";

				} else {
					value = tuple.get(i).column == null ? "NULL" : tuple.get(i).column;
				}
				switch (category) {
				case DOUBLE_GROUP:

					// For real numbers, format the string value to have 3
					// digits
					// after the point. THIS IS TOTALLY ARBITRARY and can be
					// improved to be CONFIGURABLE.
					if (!value.equals("NULL")) {
						Double dValue = Double.parseDouble(tuple.get(i).column);
						value = String.format("%.3f", dValue);
					}
					break;

				case STRING_GROUP:

					// Left justify the text columns
					c.justifyLeft();

					// and apply the width limit
					if (value.length() > maxStringColWidth) {
						value = value.substring(0, maxStringColWidth - 3) + "...";
					}
					break;
				}

				// Adjust the column width
				c.setWidth(value.length() > c.getWidth() ? value.length() : c.getWidth());
				c.addValue(value);
			}
			rowCount++;

		}

		StringBuilder strToPrint = new StringBuilder();
		StringBuilder rowSeparator = new StringBuilder();

		/*
		 * Prepare column labels to print as well as the row separator. It
		 * should look something like this:
		 * +--------+------------+------------+-----------+ (row separator) |
		 * EMP_NO | BIRTH_DATE | FIRST_NAME | LAST_NAME | (labels row)
		 * +--------+------------+------------+-----------+ (row separator)
		 */

		int j = 1;
		// Iterate over columns
		for (Field c : fields) {
			if (ordinalPostions == null || ordinalPostions.contains(j)) {
				int width = c.getWidth();

				// Center the column label
				String toPrint;
				String name = c.getLabel();
				int diff = width - name.length();

				if ((diff % 2) == 1) {
					// diff is not divisible by 2, add 1 to width (and diff)
					// so that we can have equal padding to the left and right
					// of the column label.
					width++;
					diff++;
					c.setWidth(width);
				}

				int paddingSize = diff / 2;
				String padding = new String(new char[paddingSize]).replace("\0", " ");

				toPrint = "| " + padding + name + padding + " ";

				strToPrint.append(toPrint);

				rowSeparator.append("+");
				rowSeparator.append(new String(new char[width + 2]).replace("\0", "-"));
			}
			j++;
		}

		String lineSeparator = System.getProperty("line.separator");

		lineSeparator = lineSeparator == null ? "\n" : lineSeparator;

		rowSeparator.append("+").append(lineSeparator);

		strToPrint.append("|").append(lineSeparator);
		strToPrint.insert(0, rowSeparator);
		strToPrint.append(rowSeparator);

		StringJoiner sj = new StringJoiner(", ");
		for (String name : tableNames) {
			sj.add(name);
		}

		// Print out the formatted column labels
		System.out.print(strToPrint.toString());

		String format;

		// Print out the rows
		j = 1;
		for (int i = 0; i < rowCount; i++) {
			for (Field c : fields) {
				if (ordinalPostions == null || ordinalPostions.contains(j)) {

					// format string like: "%-60s"
					format = String.format("| %%%s%ds ", c.getJustifyFlag(), c.getWidth());
					System.out.print(String.format(format, c.getValue(i)));
				}
				j++;
			}
			j = 1;
			System.out.println("|");
			System.out.print(rowSeparator);
		}
		System.out.println(resultSet.size() + " rows in set");
		System.out.println();

	}

	private static int getGroup(String type) {
		switch (sqlTypes.get(type)) {
		case Types.BIGINT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return INTEGER_GROUP;

		case Types.REAL:
		case Types.DOUBLE:
		case Types.DECIMAL:
			return DOUBLE_GROUP;

		case Types.DATE:
			return DATETIME_GROUP;

		case Types.VARCHAR:
			return STRING_GROUP;

		default:
			return OTHER;
		}
	}
}
