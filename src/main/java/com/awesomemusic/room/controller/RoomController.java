package com.awesomemusic.room.controller;

import com.awesomemusic.room.dto.RoomRequest;
import com.awesomemusic.room.dto.RoomResponse;
import com.awesomemusic.room.service.RoomService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import java.util.List;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/rooms")
@RequiredArgsConstructor
public class RoomController {

	private final RoomService service;

	@GetMapping
	@PreAuthorize("hasAnyRole('USER', 'ADMIN')")
	public List<RoomResponse> getAll() {
		return service.getAll();
	}

	@GetMapping("/{roomCode}")
	@PreAuthorize("hasAnyRole('USER', 'ADMIN')")
	public RoomResponse getByCode(@PathVariable String roomCode) {
		return service.getByCode(roomCode);
	}
	
	@PostMapping
	@PreAuthorize("hasRole('ADMIN')")
	public RoomResponse create(@RequestBody @Valid RoomRequest request) {
		return service.create(request);
	}
	
	@GetMapping("/exist/{roomCode}")
	@PreAuthorize("hasAnyRole('USER', 'ADMIN')")
	public ResponseEntity<Boolean> exists(@PathVariable String roomCode) {
	    return ResponseEntity.ok(service.exist(roomCode));
	}
}
