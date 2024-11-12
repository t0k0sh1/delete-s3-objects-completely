import boto3
from botocore.exceptions import ClientError


def delete_objects_with_prefix(bucket_name, prefix):
    s3_client = boto3.client('s3')

    try:
        # バージョンのリストをバッチで取得
        paginator = s3_client.get_paginator('list_object_versions')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        count = 0
        delete_batches = []
        for page in page_iterator:
            # 現在のページにあるバージョン付きオブジェクトを削除リストに追加
            if 'Versions' in page:
                for version in page['Versions']:
                    delete_batches.append({'Key': version['Key'], 'VersionId': version['VersionId']})
            if 'DeleteMarkers' in page:
                for delete_marker in page['DeleteMarkers']:
                    delete_batches.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

            # 1000件ごとにバッチ削除を実行
            if len(delete_batches) >= 1000:
                delete_objects_batch(s3_client, bucket_name, delete_batches)
                delete_batches = []  # バッチをクリア
                count += 1000
                print(f"{count}件のオブジェクトを削除しました。")

        # 残りの削除
        if delete_batches:
            delete_objects_batch(s3_client, bucket_name, delete_batches)
            count += len(delete_batches)
            print(f"{count}件のオブジェクトを削除しました。")

        print("削除が完了しました。")

    except ClientError as e:
        print(f"エラーが発生しました: {e}")


def delete_objects_batch(s3_client, bucket_name, delete_batches):
    try:
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': delete_batches,
                'Quiet': True
            }
        )
    except ClientError as e:
        print(f"バッチ削除中にエラーが発生しました: {e}")


# 使用例
bucket_name = 'your-bucket-name'
prefix = 'your/folder/path'
delete_objects_with_prefix(bucket_name, prefix)
