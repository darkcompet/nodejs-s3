
export interface Config {
	ACCESS_KEY: string;
	SECRET_KEY: string;
	REGION: string;
}

export interface UploadFileResult {
	result: AWS.S3.PutObjectOutput | null;
	error: AWS.AWSError | null
}

export interface DownloadFileResult {
}

export interface DeleteFileResult {
}
