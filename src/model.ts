
export interface Config {
	ACCESS_KEY: string;
	SECRET_KEY: string;
	REGION: string;
}

export interface ListObjectResult {
	data: AWS.S3.ListObjectsV2Output | null;
	err: AWS.AWSError | null;
}

export interface UploadFileResult {
	data: AWS.S3.ManagedUpload.SendData | null;
	err: Error | null;
}

export interface PutFileResult {
	data: AWS.S3.PutObjectOutput | null;
	err: AWS.AWSError | null
}

export interface DownloadFileResult {
}

export interface DeleteFileResult {
}
