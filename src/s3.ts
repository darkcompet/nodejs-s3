import AWS from "aws-sdk";
import { DeleteObjectsRequest, GetObjectRequest, ListObjectsV2Request } from "aws-sdk/clients/s3";

import fs from "fs";
import fs_promise from "fs/promises";
import path from "path";

import { DkFiles, DkUnixShell } from "@darkcompet/nodejs-core";

import * as Model from "./model";

export class DkS3 {
	private readonly s3: AWS.S3;
	private readonly config: Model.Config;

	constructor(config: Model.Config) {
		this.config = config;

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
	async ListObjects(bucketName: string, objectPrefix: string) {
		const params_list = {
			Bucket: bucketName,
			Prefix: objectPrefix,
		};

		const result = await this.s3.listObjectsV2(params_list).promise();

		result.Contents?.forEach((content) => {
			console.log("File name: " + content.Key);
		});
	}

	/**
	 * Upload a folder to s3.
	 *
	 * @param bucketName Target root folder, for eg,. isky
	 * @param objectPrefix Prefix of object path, for eg,. wm/test
	 */
	async UploadFolder(buckerName: string, fromLocalFolderPath: string, toS3RelativeFolderPath: string): Promise<Model.UploadFileResult> {
		// Get of list of files from 'dist' directory
		const fileNames = await fs_promise.readdir(fromLocalFolderPath);
		if (!fileNames || fileNames.length === 0) {
			console.log(`Aborted. Folder '${fromLocalFolderPath}' is empty or does not exist.`);
			return {};
		}
		if (fileNames.length > 10000) {
			console.error(`Aborted. It is recommended to upload under 10k files since S3 time-diff is only 15 minutes`);
			return {};
		}

		// Upload each file
		let progress = 0;
		let totalFileCount = fileNames.length;
		for (const fileName of fileNames) {
			const filePath = path.join(fromLocalFolderPath, fileName);

			// Ignore folder, non-png
			if (await DkUnixShell.DirectoryExisted(filePath)) {
				--totalFileCount;
				continue;
			}
			if (!fileName.endsWith(".png")) {
				--totalFileCount;
				continue;
			}

			// Async put to s3
			const failedFilePaths: any[] = [];
			const fileBuffer = await fs_promise.readFile(filePath);

			const params_upload = {
				Bucket: buckerName,
				Key: `${toS3RelativeFolderPath}/${fileName}`,
				Body: fileBuffer
			};

			// Start upload async
			this.s3.putObject(params_upload, (err, data) => {
				++progress;

				if (err) {
					failedFilePaths.push(filePath);
					console.error(`[${progress}/${totalFileCount}] Could NOT upload ${fileName}, error: ${JSON.stringify(err)}`);
				}
				else {
					console.log(`[${progress}/${totalFileCount}] Uploaded ${fileName}`);
				}

				if (progress == totalFileCount) {
					if (failedFilePaths.length > 0) {
						console.error("Failed file paths: " + failedFilePaths.join(", "));
					}
					else {
						console.log(`Uploaded ${totalFileCount} files successfully !`);
					}
				}
			});

			// For continuous upload:
			// const uploadResult = await s3.putObject(uploadParams).promise();
			// console.log("uploadResult: " + JSON.stringify(uploadResult));
		}

		return {};
	}

	/**
	 * Download all objects (file/folder) inside given prefix and save to local folder.
	 *
	 * @param bucketName Target root folder, for eg,. isky
	 * @param objectPrefix Prefix of object path, for eg,. wm/test
	 */
	async DownloadObjects(bucketName: string, out_dirPath: string, objectPrefix: string) {
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
	async DeleteObject(bucketName: string, objectPrefix: string) {
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
			await this.DeleteObject(bucketName, objectPrefix);
		}
	}
}
