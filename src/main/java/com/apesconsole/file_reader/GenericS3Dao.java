package com.apesconsole.file_reader;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@Slf4j
@Service
public class GenericS3Dao {

	private static final int PART_SIZE = 5 * 1024 * 1024; // 5 MB

	@Value("${s3.simulator.bucket-name}")
	private String bucketName;

	@Value("${s3.simulator.data-path}")
	private String s3DataPath;

	@Autowired
	private S3Client minioS3Client;

	@Autowired
	private S3AsyncClient minioS3AsycClient;

	public void genericFileOps(String fileName) {

		long putStart = System.currentTimeMillis();
		PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(s3DataPath + fileName)
				.build();
		minioS3Client.putObject(putObjectRequest, Paths.get(fileName));
		long putEnd = System.currentTimeMillis();

		log.info("##################################################");
		log.info("genericFileOps - Results");
		log.info("- Upload Time:" + (putEnd - putStart));
		log.info("##################################################");
	}

	public void mpFileOps(String fileName) {
		long putStart = System.currentTimeMillis();
		String uploadId = initiateMultipartUpload(minioS3AsycClient, bucketName, s3DataPath + fileName);
		if (uploadId == null) {
			System.err.println("Failed to initiate multipart upload.");
			return;
		}

		List<CompletableFuture<CompletedPart>> futures = uploadParts(minioS3AsycClient, Path.of(fileName), bucketName,
				s3DataPath + fileName, uploadId);
		if (futures == null) {
			System.err.println("Failed to upload parts.");
			return;
		}

		completeMultipartUpload(minioS3AsycClient, bucketName, s3DataPath + fileName, uploadId, futures);

		long putEnd = System.currentTimeMillis();
		log.info("##################################################");
		log.info("genericFileOps - Results");
		log.info("- Upload Time:" + (putEnd - putStart));
		log.info("##################################################");
	}

	private static String initiateMultipartUpload(S3AsyncClient s3Client, String bucketName, String objectKey) {
		try {
			CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
					.bucket(bucketName).key(objectKey).build();

			return s3Client.createMultipartUpload(createMultipartUploadRequest).join().uploadId();
		} catch (S3Exception e) {
			System.err.println("Error initiating multipart upload: " + e.getMessage());
			return null;
		}
	}

	private static List<CompletableFuture<CompletedPart>> uploadParts(S3AsyncClient s3Client, Path filePath,
			String bucketName, String objectKey, String uploadId) {
		List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
		AtomicInteger partNumber = new AtomicInteger(1);

		try (FileInputStream inputStream = new FileInputStream(filePath.toFile())) {
			byte[] buffer = new byte[PART_SIZE];
			int bytesRead;

			while ((bytesRead = inputStream.read(buffer)) != -1) {
				byte[] partData = (bytesRead == PART_SIZE) ? buffer : java.util.Arrays.copyOf(buffer, bytesRead);
				UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(bucketName).key(objectKey)
						.uploadId(uploadId).partNumber(partNumber.get()).build();

				CompletableFuture<CompletedPart> future = s3Client
						.uploadPart(uploadPartRequest, AsyncRequestBody.fromBytes(partData))
						.thenApply(uploadPartResponse -> {
							CompletedPart completedPart = CompletedPart.builder()
									.partNumber(uploadPartRequest.partNumber()).eTag(uploadPartResponse.eTag()).build();
							System.out.println(
									"Uploaded part " + uploadPartRequest.partNumber() + ": " + completedPart.eTag());
							return completedPart;
						}).exceptionally(e -> {
							System.err.println(
									"Error uploading part " + uploadPartRequest.partNumber() + ": " + e.getMessage());
							return null;
						});
				partNumber.getAndIncrement();
				futures.add(future);
			}
		} catch (IOException e) {
			System.err.println("Error reading file: " + e.getMessage());
			return null;
		}

		return futures;
	}

	private static void completeMultipartUpload(S3AsyncClient s3Client, String bucketName, String objectKey,
			String uploadId, List<CompletableFuture<CompletedPart>> futures) {
		try {
			List<CompletedPart> completedParts = new ArrayList<>();
			for (CompletableFuture<CompletedPart> future : futures) {
				CompletedPart part = future.join();
				if (part != null) {
					completedParts.add(part);
				}
			}

			// Ensure completed parts are ordered correctly
			completedParts.sort((p1, p2) -> Integer.compare(p1.partNumber(), p2.partNumber()));

			CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts)
					.build();

			CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
					.bucket(bucketName).key(objectKey).uploadId(uploadId).multipartUpload(completedMultipartUpload)
					.build();

			s3Client.completeMultipartUpload(completeMultipartUploadRequest).join();
			futures.get(0);
			System.out.println("Multipart upload completed successfully.");
		} catch (Exception e) {
			System.err.println("Error completing multipart upload: " + e.getMessage());
		}
	}

}
