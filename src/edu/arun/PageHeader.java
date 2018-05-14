package edu.arun;

import java.io.Serializable;

public class PageHeader implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = -4926523570908806110L;
	byte pageType;
	byte numberOfCells;
	short offset;
	int rightPointer;
	short[] cellLocations;
	int lengthOfHeader;
}
