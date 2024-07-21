package com.apesconsole.file_reader;

import java.net.URI;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3Configuration {

	@Value("${s3.simulator.host}")
	private String host;

	@Value("${s3.simulator.access-key}")
	private String accessKey;

	@Value("${s3.simulator.secret-key}")
	private String secretKey;

	@Bean
	public S3Client minioS3Client() {
		AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
		return S3Client.builder().endpointOverride(URI.create(host))
				.credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
				// MinIO doesn't actually use regions, but the SDK requires it
				.region(Region.US_EAST_1).build();
	}

	@Bean
	public S3AsyncClient minioS3AsycClient() {
		AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
		return S3AsyncClient.builder().region(Region.US_EAST_1)
				.credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
				.httpClientBuilder(NettyNioAsyncHttpClient.builder()).endpointOverride(URI.create(host)).build();
	}

}
