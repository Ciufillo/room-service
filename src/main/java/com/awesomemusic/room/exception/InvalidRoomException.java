package com.awesomemusic.room.exception;

public class InvalidRoomException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public InvalidRoomException(String message) {
        super(message);
    }
}
