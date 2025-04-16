package com.awesomemusic.room.service;

import com.awesomemusic.room.constants.ErrorMessages;
import com.awesomemusic.room.dto.RoomRequest;
import com.awesomemusic.room.dto.RoomResponse;
import com.awesomemusic.room.mapper.RoomMapper;
import com.awesomemusic.room.model.RoomEntity;
import com.awesomemusic.room.repository.RoomRepository;
import com.awesomemusic.room.exception.InvalidRoomException;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Service
public class RoomService {

	private final RoomRepository repository;
	private final RoomMapper mapper;

	public RoomService(RoomRepository repository, RoomMapper mapper) {
		this.repository = repository;
		this.mapper = mapper;
	}

	public List<RoomResponse> getAll() {
		return mapper.toDtoList(repository.findAll());
	}

	public RoomResponse getByCode(String roomCode) {
		Optional<RoomEntity> roomOpt = repository.findByRoomCode(roomCode);
		if (!roomOpt.isPresent()) {
			throw new InvalidRoomException(ErrorMessages.INVALID_ROOM);
		}
		return mapper.toDto(roomOpt.get());
	}

	public RoomResponse create(RoomRequest request) {
		try {
			RoomEntity room = mapper.toEntity(request);
			repository.save(room);
			return mapper.toDto(room);
		} catch (Exception e) {
			throw new InvalidRoomException(ErrorMessages.INVALID_ROOM);
		}
	}

	public boolean exist(String roomCode) {
		return repository.existsByRoomCode(roomCode);
	}
}
