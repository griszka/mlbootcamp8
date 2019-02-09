import os
import urllib
import sys
import argparse
import urllib
import datetime
import random
import requests


######################################################## BASIC CONSTANTS ########################################################
BASE_PATH = './'

DATES = list(map(lambda x: x.strftime('date=%Y-%m-%d'), [datetime.datetime(2018, 2, 1) + datetime.timedelta(days=x) for x in range(49)]))
DATES.remove('date=2018-02-11')

TRAIN = 'train'
TEST = 'test'

COLLAB = 'collab'
IMAGES = 'images'
TEXTS = 'texts'

SERVERS = list(map(str, [14, 38]))
BASE_URL = 'https://clocloSERVER.datacloudmail.ru/weblink/view/A9cF/bVSWJCJgt/'
#################################################################################################################################


################################################### PARQUET RELATED CONSTANTS ###################################################
PARQUET_PARTS_NUMBER = 7
PARQUET_PARTS_SUFFIXES = list(map(str, list(range(PARQUET_PARTS_NUMBER))))

PARQUET_TEMPLATE_URLS = {}

PARQUET_TEMPLATE_URLS[COLLAB] = {}
PARQUET_TEMPLATE_URLS[COLLAB][TRAIN] = 'collabTrain/DATE/part-0000PART-a3ee4273-5f2a-4454-9da3-2917f9345a30.c000.gz.parquet'
PARQUET_TEMPLATE_URLS[COLLAB][TEST] = 'collabTest/part-0000PART-6d949390-48b0-4104-a477-39e306b726c5-c000.gz.parquet'

PARQUET_TEMPLATE_URLS[IMAGES] = {}
PARQUET_TEMPLATE_URLS[IMAGES][TRAIN] = 'imagesTrain/DATE/part-0000PART-307db734-7475-4df5-8477-b0a0f0fb3281.c000.gz.parquet'
PARQUET_TEMPLATE_URLS[IMAGES][TEST] = 'imagesTest/part-0000PART-0dfe042a-c68d-49ef-9d64-20cca8775a05-c000.gz.parquet'

PARQUET_TEMPLATE_URLS[TEXTS] = {}
PARQUET_TEMPLATE_URLS[TEXTS][TRAIN] = 'textsTrain/DATE/part-0000PART-d4292dc4-f7d9-4fe8-8df4-a480628e583c.c000.gz.parquet'
PARQUET_TEMPLATE_URLS[TEXTS][TEST] = 'textsTest/part-0000PART-b530ebcd-5cdf-4e1c-8099-65ebf5729ba1-c000.gz.parquet'
#################################################################################################################################


################################################### IMAGES RELATED CONSTANTS ####################################################
IMAGES_THUMBNAILS_TRAIN_URL = 'imagesThumbnails/trainThumbnails.tar'
IMAGES_THUMBNAILS_TEST_URL = 'imagesThumbnails/testThumbnails.tar'

IMAGES_LARGE_TRAIN_SUFFIXES = list(map(str, list(range(10)))) + ['a', 'b', 'c', 'd', 'e', 'f']
IMAGES_LARGE_TRAIN_URLS = list(map(lambda x: 'imagesLarge/trainImages_SUFFIX.tar'.replace('SUFFIX', x), IMAGES_LARGE_TRAIN_SUFFIXES))
IMAGES_LARGE_TEST_URL = 'imagesLarge/testImages.tar'
#################################################################################################################################


#################################################### TEXTS RELATED CONSTANTS ####################################################
TEXTS_SUFFIXES = list(map(lambda x: '0' * (2 - len(str(x))) + str(x), list(range(28))))
# there are 27 chunks in terain and 28 in test
TEXTS_TRAIN_URLS = list(map(lambda x: 'texts/textsTrain/part-000PART-1b50c8f5-87db-4a53-9677-17f1113c3f8d-c000.gz.parquet'.replace('PART', x), TEXTS_SUFFIXES[:-1])) 
TEXTS_TEST_URLS = list(map(lambda x: 'texts/textsTest/part-000PART-405564e3-add3-4ad3-b54d-09ba9145fdc4-c000.gz.parquet'.replace('PART', x), TEXTS_SUFFIXES))
#################################################################################################################################


def download_from_url(url, path):
    print('Downloading file:', path)
    with open(path, 'wb') as f:
        response = requests.get(url, stream=True)
        total = response.headers.get('content-length')

        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                downloaded += len(data)
                f.write(data)
                done = int(50*downloaded/total)
                sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50-done)))
                sys.stdout.flush()
    sys.stdout.write('\n')
    print()


def download_file(url, work_path, replace):
    url = BASE_URL + url
    url = url.replace('SERVER', random.choice(SERVERS))
    file_path = os.path.join(work_path, *url.split('/')[7:])
    if os.path.isfile(file_path) and not replace:
        print('File', file_path, 'already exists and you chose not to replace already downloaded files, skipping...')
    else:
        download_from_url(url, file_path)


def generate_parquet_download_urls(track):
    train_template = PARQUET_TEMPLATE_URLS[track][TRAIN]
    test_template = PARQUET_TEMPLATE_URLS[track][TEST]

    urls = []

    # train
    for date in DATES:
        urls += list(map(lambda part: train_template.replace('DATE', date).replace('PART', part), PARQUET_PARTS_SUFFIXES))

    # test
    urls += list(map(lambda part: test_template.replace('PART', part), PARQUET_PARTS_SUFFIXES))

    return urls


def get_work_path(track):
    return os.path.join(BASE_PATH, track)


def create_folders_structure(track):
    def makedir(path):
        '''
        Since some versions of Python 3.X do not have exist_ok keyword
        '''
        try:
            os.mkdir(path)
        except:
            pass

    work_path = get_work_path(track)
    makedir(work_path)

    train_path = os.path.join(work_path, track + TRAIN.capitalize())
    makedir(train_path)


    for date in DATES:
        date_path = os.path.join(train_path, date)
        makedir(date_path)

    test_path = os.path.join(work_path, track + TEST.capitalize())
    makedir(test_path)

    if track == IMAGES:
        images_thumbnails_path = os.path.join(work_path, 'imagesThumbnails')
        makedir(images_thumbnails_path)

        images_large_path = os.path.join(work_path, 'imagesLarge')
        makedir(images_large_path)

    if track == TEXTS:
        texts_path = os.path.join(work_path, TEXTS)
        makedir(texts_path)
        texts_train_path = os.path.join(texts_path, TEXTS + TRAIN.capitalize())
        makedir(texts_train_path)
        texts_test_path = os.path.join(texts_path, TEXTS + TEST.capitalize())
        makedir(texts_test_path)

    print('All necessary folders created')


def download_parquet(track, replace):
    print('Downloading parquet for', track, 'track:')
    work_path = get_work_path(track)
    urls = generate_parquet_download_urls(track)
    for url in urls:
        download_file(url, work_path, replace)


def download_images_thumbnail(work_path, replace):
    print('Downloading images thumbnails:')
    urls = [IMAGES_THUMBNAILS_TRAIN_URL, IMAGES_THUMBNAILS_TEST_URL]
    for url in urls:
        download_file(url, work_path, replace)


def download_images_large(work_path, replace):
    print('Downloading large images:')
    urls = IMAGES_LARGE_TRAIN_URLS + [IMAGES_LARGE_TEST_URL]
    for url in urls:
        download_file(url, work_path, replace)


def download_texts(work_path, replace):
    print('Downloading texts:')
    urls = TEXTS_TRAIN_URLS + TEXTS_TEST_URLS
    for url in urls:
        download_file(url, work_path, replace)


def download_data(track, replace, download_large_images, parquet_only):
    print('Downloading data for', track, 'track')

    create_folders_structure(track)
    download_parquet(track, replace)

    if track == IMAGES and not parquet_only:
        work_path = get_work_path(track)
        if download_large_images:
            download_images_large(work_path, replace)
        else:
            download_images_thumbnail(work_path, replace)

    if track == TEXTS and not parquet_only:
        work_path = get_work_path(track)
        download_texts(work_path, replace)


def main():
    parser = argparse.ArgumentParser(description='Script for downloading data for mlbootcamp 8 / SNA hackathon 2019. Usage example: \
        "python3 data_downloader.py -t images -b /home/username/ml8/ -l" \
        Command above will create folder /home/username/ml8/images and download parquet data along with large version of images to that folder. \
        For more options see below help. Runs with python3. Somehow tested for Unix. Not tested for Windows (os.path.join is used for path builidng though). \
        Network exceptions are not handled. Download logic taken from https://sumit-ghosh.com/articles/python-download-progress-bar/. Any testing/help is welcome.')
    parser.add_argument('-t', '--track', type=str, dest='track',
                        help='Tracks which data you want to download',
                        required=True, choices=[COLLAB, IMAGES, TEXTS])
    parser.add_argument('-r', '--replace', type=bool, dest='replace', default=False,
                        help='Whether re-downloaded already existing files')
    parser.add_argument('-b', '--base_path', type=str, dest='base_path', default='./',
                        help='Base path for your data storage')
    parser.add_argument('-l', '--download_large_images', dest='download_large_images', default=False, action='store_true',
                        help='Whether to download large images (thumbnails will not be downloaded) in case of images track')
    parser.add_argument('-p', '--parquet_only', dest='parquet_only', default=False, action='store_true',
                        help='Whether to download parquet (metadata) files only in case of images or texts track')

    args = parser.parse_args()

    global BASE_PATH
    BASE_PATH = args.base_path

    download_data(args.track, args.replace, args.download_large_images, args.parquet_only)


if __name__ == "__main__":
    main()
