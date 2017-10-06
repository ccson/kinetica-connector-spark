package com.kinetica.exception;

import java.io.Serializable;

public class KineticaException extends RuntimeException implements Serializable {

	private static final long serialVersionUID = 3064406863292282746L;

	public KineticaException() {
		super();
	}

	public KineticaException(String message, Throwable cause) {
		super(message, cause);
	}

	public KineticaException(String message) {
		super(message);
	}

	public KineticaException(Throwable cause) {
		super(cause);
	}	
}