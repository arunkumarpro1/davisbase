package edu.arun;

public class SerialTypeCode {
	byte typeCode;
	byte length;

	public SerialTypeCode(int typeCode, int length) {
		super();
		this.typeCode = (byte) typeCode;
		this.length = (byte) length;
	}
}
