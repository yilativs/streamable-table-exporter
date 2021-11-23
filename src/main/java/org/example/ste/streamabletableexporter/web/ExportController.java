package org.example.ste.streamabletableexporter.web;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.example.ste.streamabletableexporter.dao.StreamableTableToCvsExportRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
@RequestMapping(value = "/export")
public class ExportController {

	private final StreamableTableToCvsExportRepository repository;

	public ExportController(StreamableTableToCvsExportRepository repository) {
		this.repository = repository;
	}

	@GetMapping(value = "/csv")
	public ResponseEntity<StreamingResponseBody> export() {
		StreamingResponseBody responseBody = httpResponseOutputStream -> {
			try (Writer writer = new BufferedWriter(new OutputStreamWriter(httpResponseOutputStream))) {
				repository.export("demo.execution").forEach(line -> {
					try {
						writer.write(line);
						writer.write('\n');
						writer.flush();
					} catch (IOException e) {
						throw new RuntimeException(e.getMessage());
					}
				});
			}
		};
		MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
		headers.add("content-type", "text/csv");
		return new ResponseEntity<>(responseBody, headers, HttpStatus.OK);
	}

}
