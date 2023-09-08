import os
import sys

import boto3
from PIL import Image
from io import BytesIO
import logging


class S3ImageCompressor:
    def __init__(self, awsAccessKey, awsSecretKey, s3BucketName, inputDirectory, outputDirectory):
        self.awsAccessKey = awsAccessKey
        self.awsSecretKey = awsSecretKey
        self.s3BucketName = s3BucketName
        self.inputDirectory = inputDirectory
        self.outputDirectory = outputDirectory

        # Initialize the S3 client
        self.s3_client = boto3.client('s3', aws_access_key_id=awsAccessKey, aws_secret_access_key=awsSecretKey)

    @staticmethod
    def compressImage(imagePath):
        """
                Compresses an image using Pillow while maintaining quality.
        """
        try:
            with Image.open(imagePath) as img:
                # Save the compressed image to a temporary buffer
                buffer = BytesIO()
                img.save(buffer, format=img.format, optimize=True,
                         quality=95)  # You can adjust the format and quality as needed
                return buffer.getvalue()
        except Exception as e:
            errorType = f"Error compressing {imagePath}: {str(e)}"
            print(errorType)
            logging.error(f"compressImage: {errorType}")
            return None

    def processS3Directory(self):
        """
                Traverses a directory, compresses image files, and uploads them to S3.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.s3BucketName, Prefix=self.inputDirectory)

            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith(('.jpg', '.jpeg', '.png')):
                    # Download the image from S3
                    s3_object = self.s3_client.get_object(Bucket=self.s3BucketName, Key=key)
                    image_data = s3_object['Body'].read()

                    # Compress the image
                    compressed_image_data = self.compressImage(BytesIO(image_data))

                    if compressed_image_data:
                        # Upload the compressed image to the output directory within S3
                        output_key = key.replace(self.inputDirectory, self.outputDirectory)
                        self.s3_client.put_object(Bucket=self.s3BucketName, Key=output_key,
                                                  Body=compressed_image_data)

                        print(f"Compressed and uploaded: {output_key}")
                    else:
                        logging.error(f"processS3Directory: not uploaded this {self.inputDirectory} images")

        except Exception as e:
            errorType = f"Error processing S3 directory: {str(e)}"
            print(f"Error processing S3 directory: {str(e)}")
            logging.error(f"processS3Directory: {errorType}")


if __name__ == "__main__":
    aws_access_key = 'AWS Access Key'
    aws_secret_key = 'AWS Secret key'
    # s3_bucket_name = 'moviesgarhs'
    s3_bucket_name = None
    if len(sys.argv) > 1:
        s3_bucket_name = sys.argv[1]
        directoryInsideS3Bucket = "directoryName"
        s3_directory_path = f'{s3_bucket_name}/{directoryInsideS3Bucket}'  # Replace with the path to your input directory
        output_directory_path = f'{s3_bucket_name}/{directoryInsideS3Bucket}'  # Replace with the path to your output directory
        # in output_directory_path you can store in another directory for this you have change directory name for output

        # Create an instance of the S3ImageCompressor class
        image_compressor = S3ImageCompressor(aws_access_key, aws_secret_key, s3_bucket_name, s3_directory_path,
                                             output_directory_path)

        # Call the process_s3_directory method to process images in the S3 directory
        image_compressor.processS3Directory()
    else:
        print("Please Enter S3 Bucket Name")
