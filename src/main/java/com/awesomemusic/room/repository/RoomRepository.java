package com.awesomemusic.room.repository;

import com.awesomemusic.room.model.RoomEntity;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

public interface RoomRepository extends JpaRepository<RoomEntity, Long> {

	Optional<RoomEntity> findByRoomCode(String roomCode);
	
	boolean existsByRoomCode(String roomCode);
}
