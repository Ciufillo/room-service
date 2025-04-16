package com.awesomemusic.room.controller;

import com.awesomemusic.room.dto.RoomResponse;
import com.awesomemusic.room.service.RoomService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.Mockito.doReturn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.ArrayList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest
@AutoConfigureMockMvc
class RoomControllerTest {

	@Autowired
	MockMvc mockMvc;
	
	@MockBean
	RoomService service;
	
    @Autowired
    ObjectMapper objectMapper;
	
	String baseUrl = "/api/rooms";
	String X_ROLE = "X-ROLE";
	String ROOM_CODE = "SALA_A";

	@Test
	void getAllAdminTest() throws Exception {
		doReturn(new ArrayList<RoomResponse>()).when(service).getAll();
		mockMvc.perform(get(baseUrl)
				.header(X_ROLE, "admin"))
			.andExpect(status().isOk());
	}
	
	@Test
	void getAllUserTest() throws Exception {
		doReturn(new ArrayList<RoomResponse>()).when(service).getAll();
		mockMvc.perform(get(baseUrl)
				.header(X_ROLE, "user"))
			.andExpect(status().isOk());
	}

	@Test
	void getByCodeAdminTest() throws Exception {
		doReturn(new RoomResponse()).when(service).getByCode(anyString());
		mockMvc.perform(get(baseUrl + "/" + ROOM_CODE)
				.header(X_ROLE, "admin"))
			.andExpect(status().isOk());
	}
	
	@Test
	void getByCodeUserTest() throws Exception {
		doReturn(new RoomResponse()).when(service).getByCode(anyString());
		mockMvc.perform(get(baseUrl + "/" + ROOM_CODE)
				.header(X_ROLE, "user"))
			.andExpect(status().isOk());
	}
	
	@Test
	void createAdminTest() throws Exception {
		String body = "{\"roomCode\":\"SALA_C\",\"name\":\"Studio C\",\"description\":\"Sala da dj\"}";
		doReturn(new RoomResponse()).when(service).getByCode(any());
		mockMvc.perform(post(baseUrl)
				.header(X_ROLE, "admin")
				.content(body)
                .contentType(MediaType.APPLICATION_JSON))
			.andExpect(status().isOk());
	}
	
	@Test
	void createUserTest() throws Exception {
		String body = "{\"roomCode\":\"SALA_C\",\"name\":\"Studio C\",\"description\":\"Sala da dj\"}";
		doReturn(new RoomResponse()).when(service).getByCode(any());
		mockMvc.perform(post(baseUrl)
				.header(X_ROLE, "user")
				.content(body)
                .contentType(MediaType.APPLICATION_JSON))
			.andExpect(status().isForbidden());
	}
	
	@Test
	void createValidationErrorTest() throws Exception {
		String body = "{\"roomCode\":null,\"name\":null,\"description\":null}";
		doReturn(new RoomResponse()).when(service).getByCode(any());
		mockMvc.perform(post(baseUrl)
				.header(X_ROLE, "admin")
				.content(body)
                .contentType(MediaType.APPLICATION_JSON))
			.andExpect(status().isBadRequest());
	}
	
	@Test
	void existsAdminTest() throws Exception {
		doReturn(true).when(service).exist(anyString());
		mockMvc.perform(get(baseUrl + "/exist/" + ROOM_CODE)
				.header(X_ROLE, "admin"))
			.andExpect(status().isOk());
	}
	
	@Test
	void existsUserTest() throws Exception {
		doReturn(true).when(service).exist(anyString());
		mockMvc.perform(get(baseUrl + "/exist/" + ROOM_CODE)
				.header(X_ROLE, "user"))
			.andExpect(status().isOk());
	}

}
