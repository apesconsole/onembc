package com.apesconsole.file_reader;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class Simulator {
	
	@Autowired
	private GenericS3Dao genericS3Dao;

	@GetMapping("/sim/generic")
	public void simmulateGeneric() throws IOException {
		log.info("Simmulating Generic Way");
		genericS3Dao.genericFileOps("25mb.json");
	}
	
	@GetMapping("/sim/mpf")
	public void simmulateMmf() throws IOException {
		log.info("Simmulating Multipart Way");
		genericS3Dao.mpFileOps("25mb.json");
	}
}
