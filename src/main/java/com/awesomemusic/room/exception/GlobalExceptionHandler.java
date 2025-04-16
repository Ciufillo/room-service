package com.awesomemusic.room.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<ErrorResponse> handleValidationErrors(MethodArgumentNotValidException ex) {
		String message = ex.getBindingResult().getFieldErrors().stream()
				.map(error -> error.getField() + " " + error.getDefaultMessage()).collect(Collectors.joining("; "));

		ErrorResponse response = new ErrorResponse("VALIDATION_FAILED", message);
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
	}

	@ExceptionHandler(InvalidRoomException.class)
	public ResponseEntity<ErrorResponse> handleInvalidRoom(InvalidRoomException ex) {
		ErrorResponse response = new ErrorResponse("INVALID_ROOM", ex.getMessage());
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
	}
	
	public static class ErrorResponse {
		private String errorCode;
		private String message;

		public ErrorResponse(String errorCode, String message) {
			this.errorCode = errorCode;
			this.message = message;
		}

		public String getErrorCode() {
			return errorCode;
		}

		public String getMessage() {
			return message;
		}
	}
}
