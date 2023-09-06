import os
import boto3
from botocore.exceptions import NoCredentialsError
from PIL import Image
import logging
import sys

# Configure the logger
logging.basicConfig(
    filename='imagesInfo.log',  # Name of the log file
    level=logging.DEBUG,  # Log level (e.g., INFO, DEBUG, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class ImageCompressor:
    def __init__(self, accessKey, secretKey, bucketName):
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.bucketName = bucketName

        # Configure AWS credentials
        boto3.setup_default_session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.s3 = boto3.client('s3')

    def compressImage(self, inputFle, outputFile, quality=95):
        """
        Compresses an image using Pillow while maintaining quality.
        """
        try:
            img = Image.open(inputFle)
            img.save(outputFile, format=img.format, quality=quality)
        except Exception as e:
            errorType = f"compressImage: Error compressing {inputFle}: {e}"
            print(f"compressImage: Error compressing {inputFle}: {e}")
            logging.error(errorType)

    def uploadToS3(self, localPath, s3Path):
        """
        Uploads a file to an AWS S3 bucket.
        """
        try:
            self.s3.upload_file(localPath, self.bucketName, s3Path)
        except NoCredentialsError:
            errorType = 'uploadToS3: AWS credentials not found. Please configure your AWS credentials.'
            print("uploadToS3: AWS credentials not found. Please configure your AWS credentials.")
            logging.error(errorType)

    def processImages(self, directory):
        """
        Traverses a directory, compresses image files, and uploads them to S3.
        """
        for root, _, files in os.walk(directory):
            for filename in files:
                try:
                    if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                        inputPath = os.path.join(root, filename)
                        relativePath = os.path.relpath(inputPath, directory)
                        outputPath = f"compressed/{relativePath}"

                        # Create the compressed folder structure in S3
                        s3_path = f"{self.bucketName}/{outputPath}"

                        # Create the local directory for compressed images
                        # os.makedirs(os.path.dirname(outputPath), exist_ok=True)

                        # Compress the image
                        self.compressImage(inputPath, outputPath)

                        # Upload the compressed image to S3
                        self.uploadToS3(outputPath, s3_path)
                        logging.info('processImages: successfully insert image ' + str(filename))
                    else:
                        logging.critical(
                            'processImages: Image Type is different and image details is :' + str(filename))
                except Exception as error:
                    logging.error('processImages: error in uploading image', str(filename))
                    print(error)


if __name__ == "__main__":
    access_key = "access key"  # Access Key
    secret_key = "secret key"  # Secret key
    # taking bucket name from user
    if len(sys.argv) > 1:
        bucket_name = sys.argv[1]
        # bucket_name = "moviesgarhs"  # bucket name

        # Specify the directory containing images
        source_directory = "directoryImages"

        # Create an instance of the ImageCompressor class
        compressor = ImageCompressor(access_key, secret_key, bucket_name)

        # Process and upload images to S3
        compressor.processImages(source_directory)
    else:
        print("Please provide S3 bucket Name while running this script")
