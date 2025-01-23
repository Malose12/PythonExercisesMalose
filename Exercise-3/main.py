import boto3
import gzip
import io


def main():
    # your code here
    s3_client = boto3.client('s3')

    #bucket and ke for intitial file
    bucket_name = "commoncrawl"
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    #download and decompress he gz file

    response = s3_client.get_object(Bucket=bucket_name,Key=key)
    gzipped_content = response['Body'].read()

    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content)) as gz_file:
        first_line = gz_file.readline().decode('utf-8').strip()
    
    print(f'First URI: {first_line}')

    response = s3_client.get_object(Bucket=bucket_name,Key=first_line)

    for line in response['Bidy'].iter_lines():
        print(line.decode('utf-8'))

    # pass


if __name__ == "__main__":
    main()
