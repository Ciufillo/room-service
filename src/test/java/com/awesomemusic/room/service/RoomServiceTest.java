package com.awesomemusic.room.service;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import com.awesomemusic.room.dto.RoomRequest;
import com.awesomemusic.room.dto.RoomResponse;
import com.awesomemusic.room.model.RoomEntity;
import com.awesomemusic.room.repository.RoomRepository;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import com.awesomemusic.room.exception.InvalidRoomException;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

@SpringBootTest
class RoomServiceTest {
	
	@MockBean
	private RoomRepository repository;

	@Autowired
    RoomService service;
	
	String ROOM_CODE = "SALA_A";
	String ROOM_CODE_ERRATA = "SALA_99";
	String NAME = "Sala A";
	String DESCRIPTION = "Sala per Dj";
    
	@Test
	void getAllTest() {
		doReturn(getRoomsEntity()).when(repository).findAll();
		List<RoomResponse> response = service.getAll();
		assertNotNull(response);
	}
	
    public static List<RoomEntity> getRoomsEntity() {
    	RoomEntity room1 = new RoomEntity();
        room1.setRoomCode("ROOM001");
        room1.setName("Sala Rehearsal");
        room1.setDescription("Stanza per prove musicali");

        RoomEntity room2 = new RoomEntity();
        room2.setRoomCode("ROOM002");
        room2.setName("Sala Registrazione");
        room2.setDescription("Stanza con strumentazione audio");
        
        return Arrays.asList(room1, room2);
    }
	
	@Test
	void getByCodeTest() {
		//ok
		RoomEntity mockRoom = getRoomsEntity().get(0);
		doReturn(Optional.of(mockRoom)).when(repository).findByRoomCode(anyString());
		RoomResponse response = service.getByCode(ROOM_CODE);
		assertNotNull(response);
		
		//ko
		doReturn(Optional.empty()).when(repository).findByRoomCode(anyString());
		assertThrows(InvalidRoomException.class, () -> { service.getByCode(ROOM_CODE_ERRATA);});
		
	}
	
	@Test
	void createTest() {
		//ok
		RoomEntity mockRoom = getRoomsEntity().get(0);
		when(repository.save(any())).thenReturn(null);
		doReturn(Optional.of(mockRoom)).when(repository).findByRoomCode(anyString());
		RoomResponse response = service.create(createRoomRequest(ROOM_CODE, NAME, DESCRIPTION));
		assertNotNull(response);
		
		//ko
		when(repository.save(any())).thenThrow(new RuntimeException("Errore nel salvataggio"));
		doReturn(Optional.of(mockRoom)).when(repository).findByRoomCode(anyString());
		assertThrows(InvalidRoomException.class, () -> { service.create(createRoomRequest(ROOM_CODE, NAME, DESCRIPTION));});
		
	}
	
	public RoomRequest createRoomRequest(String roomCode, String name, String description) {
	    RoomRequest roomRequest = new RoomRequest();
	    roomRequest.setRoomCode(roomCode);
	    roomRequest.setName(name);
	    roomRequest.setDescription(description);
	    return roomRequest;
	}
	
	@Test
	void existTest() {
		//ok
		doReturn(true).when(repository).existsByRoomCode(anyString());
		boolean responseTrue = service.exist(ROOM_CODE);
		assertTrue(responseTrue);
		
		//ko
		doReturn(false).when(repository).existsByRoomCode(anyString());
		boolean responseFalse = service.exist(ROOM_CODE);
		assertFalse(responseFalse);
		
	}
	
	


}
