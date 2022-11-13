import AWS from "aws-sdk";
import { DeleteObjectsRequest, GetObjectRequest, ListObjectsV2Request } from "aws-sdk/clients/s3";

import fs from "fs";
import fs_promise from "fs/promises";
import path from "path";

import { DkFiles, DkUnixShell } from "@darkcompet/nodejs-core";

import * as Model from "./model";

export class DkS3 {
	private readonly s3: AWS.S3;

	constructor(config: Model.Config) {
		// Config S3
		AWS.config.update({
			accessKeyId: config.ACCESS_KEY,
			secretAccessKey: config.SECRET_KEY,
			region: config.REGION
		});

		// Create S3 client
		this.s3 = new AWS.S3({ signatureVersion: 'v4' });
	}

	/**
	 * List all objects (file/folder) inside given prefix.
	 * Max 1k objects??
	 *
	 * @param bucketName Target root folder, for eg,. isky
	 * @param objectPrefix Prefix of object path, for eg,. wm/test
	 */
	async ListObjectsAsync(bucketName: string, objectPrefix: string): Promise<Model.ListObjectResult> {
		const params_list = {
			Bucket: bucketName,
			Prefix: objectPrefix,
		};

		try {
			return {
				data: await this.s3.listObjectsV2(params_list).promise(),
				err: null
			};
			// result.Contents?.forEach((content) => {
			// 	console.log("File name: " + content.Key);
			// });
		}
		catch (e: any) {
			return { data: null, err: e };
		}
	}

	/**
	 * Upload client file to s3.
	 * This is complex process which handles arbitrary file size, retry count,... for us.
	 *
	 * @param bucketName Target root folder of s3. For eg,. staging
	 * @param clientFilePath Src file path at local client. For eg,. ./mydata/upload/clip.mp4
	 * @param s3RelativeFilePath Dst remote relative file path. For eg,. upload/today/movie.mp4
	 * @param ACL See ObjectCannedACL = "private"|"public-read"|"public-read-write"|"authenticated-read"|"aws-exec-read"|"bucket-owner-read"|"bucket-owner-full-control"|string;
	 */
	async UploadFileAsync(
		bucketName: string,
		clientFilePath: string,
		s3RelativeFilePath: string,
		ACL: string | null = null
	): Promise<Model.UploadFileResult> {
		try {
			// File must exist
			if (!(await DkUnixShell.FileExisted(clientFilePath))) {
				throw new Error("File not exist");
			}

			const fileBuffer = await fs_promise.readFile(clientFilePath);
			const uploadParams: any = {
				Bucket: bucketName,
				Key: s3RelativeFilePath,
				Body: fileBuffer,
			};

			if (ACL) {
				uploadParams.ACL = ACL;
			}

			return {
				data: await this.s3.upload(uploadParams).promise(),
				err: null
			};
		}
		catch (e: any) {
			return { data: null, err: e };
		}
	}

	/**
	 * Upload client file to s3.
	 * In general, we use it when put small object.
	 *
	 * @param bucketName Target root folder of s3. For eg,. staging
	 * @param clientFilePath Src file path at local client. For eg,. ./mydata/upload/clip.mp4
	 * @param s3RelativeFilePath Dst remote relative file path. For eg,. upload/today/movie.mp4
	 * @param ACL See ObjectCannedACL = "private"|"public-read"|"public-read-write"|"authenticated-read"|"aws-exec-read"|"bucket-owner-read"|"bucket-owner-full-control"|string;
	 */
	async PutFileAsync(
		bucketName: string,
		clientFilePath: string,
		s3RelativeFilePath: string,
		ACL: string | null = null
	): Promise<Model.PutFileResult> {
		try {
			// File must exist
			if (!(await DkUnixShell.FileExisted(clientFilePath))) {
				throw new Error("File not exist");
			}

			const fileBuffer = await fs_promise.readFile(clientFilePath);
			const uploadParams: any = {
				Bucket: bucketName,
				Key: s3RelativeFilePath,
				Body: fileBuffer
			};

			if (ACL) {
				uploadParams.ACL = ACL;
			}

			return {
				data: await this.s3.putObject(uploadParams).promise(),
				err: null
			};
		}
		catch (e: any) {
			return { data: null, err: e };
		}
	}

	/**
	 * Put given folder to s3.
	 *
	 * @param bucketName Target root folder of s3. For eg,. staging
	 * @param fromLocalFolderPath From local folder of client. For eg,. ./mydata/upload
	 * @param toS3RelativeFolderPath To s3 relative object path. For eg,. upload/today
	 */
	async PutFolderAsync(
		bucketName: string,
		fromLocalFolderPath: string,
		toS3RelativeFolderPath: string,
		eachFileCallback: (error: AWS.AWSError, data: AWS.S3.PutObjectOutput) => {}
	) {
		// Get of list of files from 'dist' directory
		const fileNames = await fs_promise.readdir(fromLocalFolderPath);
		if (!fileNames || fileNames.length === 0) {
			console.log(`Aborted. Folder '${fromLocalFolderPath}' is empty or does not exist.`);
			return;
		}
		if (fileNames.length > 10000) {
			console.error(`Aborted. It is recommended to upload under 10k files since S3 time-diff is only 15 minutes`);
			return;
		}

		// Upload each file
		for (const fileName of fileNames) {
			const filePath = path.join(fromLocalFolderPath, fileName);

			// Ignore folder
			if (await DkUnixShell.DirectoryExisted(filePath)) {
				continue;
			}

			// Async put to s3
			const fileBuffer = await fs_promise.readFile(filePath);
			const uploadParams = {
				Bucket: bucketName,
				Key: `${toS3RelativeFolderPath}/${fileName}`,
				Body: fileBuffer
			};

			// Upload multiple files by use callback instead of await
			this.s3.putObject(uploadParams, eachFileCallback);
		}
	}

	/**
	 * Download all objects (file/folder) inside given prefix and save to local folder.
	 *
	 * @param bucketName Target root folder, for eg,. isky
	 * @param objectPrefix Prefix of object path, for eg,. wm/test
	 */
	async DownloadObjectsAsync(bucketName: string, out_dirPath: string, objectPrefix: string) {
		const params: ListObjectsV2Request = {
			Bucket: bucketName,
			Prefix: objectPrefix,
		};

		const listResult = await this.s3.listObjectsV2(params).promise();
		listResult.Contents?.forEach(async (content) => {
			console.log("Going to download file: " + content.Key);

			// Download
			const downloadParams: GetObjectRequest = {
				Bucket: bucketName,
				Key: content.Key!
			};

			const out_filePath = path.join(out_dirPath, content.Key!);
			await DkFiles.MkDirsOrThrowAsync(out_dirPath);

			const readStream = this.s3.getObject(downloadParams).createReadStream();
			const writeStream = fs.createWriteStream(out_filePath);
			readStream.pipe(writeStream);
		});
	}

	/**
	 * Delete object (file/folder) that prefix with given prefix.
	 *
	 * @param bucketName Target root folder, for eg,. isky
	 * @param objectPrefix Prefix of object path, for eg,. wm/test
	 */
	async DeleteObjectAsync(bucketName: string, objectPrefix: string) {
		const params_list = {
			Bucket: bucketName,
			Prefix: objectPrefix,
		};

		// Obtain file list for deletion
		const listResult = await this.s3.listObjectsV2(params_list).promise();
		listResult.Contents?.forEach((content) => {
			console.log("Going to delete file: " + content.Key);
		});

		// Delete files
		const params_delete: DeleteObjectsRequest = {
			Bucket: bucketName,
			Delete: {
				Objects: []
			}
		};
		listResult.Contents?.forEach((content) => {
			params_delete.Delete.Objects.push({
				Key: content.Key!
			});
		});

		const deleteResult = await this.s3.deleteObjects(params_delete).promise();
		deleteResult.Deleted?.forEach((deletedObj) => {
			console.log("Deleted file: " + deletedObj.Key);
		});

		// If has more files, continue to delete
		if (listResult.IsTruncated) {
			console.log("Recursively delete remaining files...");
			await this.DeleteObjectAsync(bucketName, objectPrefix);
		}
	}
}
