package com.awesomemusic.room.mapper;

import java.util.List;
import org.mapstruct.Mapper;
import com.awesomemusic.room.dto.RoomRequest;
import com.awesomemusic.room.dto.RoomResponse;
import com.awesomemusic.room.model.RoomEntity;

@Mapper(componentModel = "spring")
public interface RoomMapper {
	
    RoomResponse toDto(RoomEntity entity);
    
    List<RoomResponse> toDtoList(List<RoomEntity> entities);
    
    RoomEntity toEntity(RoomRequest request);

}
