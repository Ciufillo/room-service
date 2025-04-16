package com.awesomemusic.room.dto;

import javax.validation.constraints.NotBlank;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class RoomRequest {
	
	@NotBlank(message = "Il codice stanza è obbligatorio")
	private String roomCode;
	
	@NotBlank(message = "Il nome è obbligatorio")
	private String name;
	
	@NotBlank(message = "La descrizione è obbligatoria")
	private String description;

	public String getRoomCode() {
		return roomCode;
	}

	public void setRoomCode(String roomCode) {
		this.roomCode = roomCode;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
