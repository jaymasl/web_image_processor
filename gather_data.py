import requests
import json
import logging
import os
import time
from datetime import datetime
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

import duckdb
from pillow_heif import register_heif_opener
from PIL import Image, ExifTags

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')

CONFIG = {
    'MAX_RETRIES': 3,
    'RETRY_DELAY': 5,
    'SIMILARITY_THRESHOLD': 0.7,
    'TIME_THRESHOLD': 1,
    'IMAGES_TO_PROCESS': 50,
    'REFRESH_INTERVAL': 1,
    'FETCH_BUFFER': 100,
    'MAX_RECENT_POSTS_PER_USER': 1,
    'API_KEY': 'api_key',
    'DB_PATH': 'images.db',
    'PROCESS_THRESHOLD': 100,
    'PAUSE_DURATION': 120,
    'MAX_DRIVER_RETRIES': 3
}

register_heif_opener()

class ImageProcessor:
    def __init__(self):
        self.session = requests.Session()
        self.webdriver_options = Options()
        self.webdriver_options.add_argument('-headless')
        self.processed_count = 0
        self.skipped_count = 0
        self.previous_images = []
        self.driver = None
        self.db_con = None
        self.existing_entries_set = set()
        self.processed_ids = set()
        self.recent_users = {}
        self.consecutive_duplicate_count = 0

    def __enter__(self):
        self.driver = self.initialize_driver()
        self.db_con = duckdb.connect(CONFIG['DB_PATH'])
        self.db_con.execute('''
            CREATE TABLE IF NOT EXISTS images (
                id BIGINT,
                url TEXT,
                hash TEXT,
                createdAt TIMESTAMP,
                postId BIGINT,
                username TEXT,
                web_url TEXT,
                tags TEXT[],
                user_comment TEXT
            )
        ''')
        existing_entries = self.db_con.execute('SELECT tags, user_comment FROM images').fetchall()
        self.existing_entries_set = {(tuple(tags), user_comment) for tags, user_comment in existing_entries}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()
        if self.db_con:
            self.db_con.close()

    def initialize_driver(self):
        retries = 0
        while retries < CONFIG['MAX_DRIVER_RETRIES']:
            try:
                return webdriver.Firefox(options=self.webdriver_options)
            except WebDriverException as e:
                retries += 1
                logging.error(f"Failed to initialize WebDriver (Attempt {retries}/{CONFIG['MAX_DRIVER_RETRIES']}) - {str(e)}")
                time.sleep(CONFIG['RETRY_DELAY'])
        raise WebDriverException("Failed to initialize WebDriver after multiple attempts.")

    def fetch_images(self, page=1):
        url = 'website'
        params = {'limit': CONFIG['FETCH_BUFFER'], 'sort': 'Newest', 'page': page}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {CONFIG['API_KEY']}"
        }
        try:
            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json().get('items', [])
            for image in data:
                image['web_url'] = f"website/{image['id']}"
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch images: {e}")
            return []

    def process_image(self, image):
        image_filename = f"temp_image_{image['id']}.jpg"
        try:
            response = self.session.get(image['url'])
            response.raise_for_status()
            with open(image_filename, 'wb') as f:
                f.write(response.content)

            user_comment = self.extract_exif_user_comment(image_filename)
            if not user_comment:
                logging.info(f"Skipping image ID: {image['id']} - No EXIF user comment found.")
                return None

            extracted_data = self.extract_image_details(image)
            if not extracted_data.get('tags'):
                logging.info(f"Skipping image ID: {image['id']} - No tags found.")
                return None

            formatted_data = self.format_image_data(image, extracted_data, user_comment)
            return formatted_data

        except Exception as e:
            logging.error(f"Error processing image ID: {image['id']} - {str(e)}")
            return None
        finally:
            if os.path.exists(image_filename):
                os.remove(image_filename)

    @staticmethod
    def extract_exif_user_comment(image_path):
        try:
            with Image.open(image_path) as img:
                exif = img._getexif()
                if not exif:
                    return None
                for tag_id, value in exif.items():
                    if ExifTags.TAGS.get(tag_id) == 'UserComment':
                        return ImageProcessor.decode_user_comment(value)
            return None
        except Exception:
            return None

    @staticmethod
    def decode_user_comment(value):
        if isinstance(value, bytes):
            value = value[8:] if value.startswith(b'UNICODE') else value
            for encoding in ['utf-16-be', 'utf-8', 'ascii']:
                try:
                    return value.decode(encoding).replace('\x00', '')
                except UnicodeDecodeError:
                    pass
        return str(value)

    def extract_image_details(self, image):
        retries = 0
        while retries < CONFIG['MAX_RETRIES']:
            try:
                self.driver.get(image['web_url'])
                WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, 'main')))
                tags = [tag.text.strip() for tag in self.driver.find_elements(By.CSS_SELECTOR, 'a.mantine-Text-root.mantine-ljqvxq') if tag.text.strip()]
                if tags:
                    return {'tags': tags}
                else:
                    raise ValueError("No tags found on page.")
            except (TimeoutException, WebDriverException) as e:
                retries += 1
                logging.warning(f"Retrying extraction for image ID: {image['id']} (Attempt {retries}/{CONFIG['MAX_RETRIES']}) - {str(e)}")
                if isinstance(e, WebDriverException):
                    self.driver.quit()
                    self.driver = self.initialize_driver()
                time.sleep(CONFIG['RETRY_DELAY'])
        logging.error(f"Failed to extract tags for image ID: {image['id']} after {CONFIG['MAX_RETRIES']} retries.")
        return {'tags': []}

    def format_image_data(self, image, extracted_data, user_comment):
        data = {
            'id': image.get('id'),
            'url': image.get('url'),
            'hash': image.get('hash'),
            'createdAt': self.standardize_date(image.get('createdAt')),
            'postId': image.get('postId'),
            'username': image.get('username'),
            'web_url': image.get('web_url'),
            'tags': extracted_data.get('tags'),
            'user_comment': user_comment
        }
        self.convert_numeric_fields(data)
        return data

    @staticmethod
    def standardize_date(date_str):
        try:
            if date_str:
                date_str = date_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(date_str)
                return dt.isoformat()
        except (TypeError, ValueError) as e:
            logging.error(f"Error parsing date '{date_str}': {e}")
        return None

    @staticmethod
    def convert_numeric_fields(d):
        for key, value in d.items():
            if isinstance(value, dict):
                ImageProcessor.convert_numeric_fields(value)
            elif isinstance(value, str):
                if value.isdigit():
                    d[key] = int(value)
                else:
                    try:
                        d[key] = float(value)
                    except ValueError:
                        pass

    def is_similar_entry(self, new_data):
        try:
            new_tags = tuple(new_data['tags'])
            new_user_comment = new_data['user_comment']
            return (new_tags, new_user_comment) in self.existing_entries_set
        except Exception as e:
            logging.error(f"Error checking for similar entries: {e}")
            return False

    def is_duplicate(self, username, user_comment):
        try:
            query = '''
            SELECT COUNT(*) 
            FROM images 
            WHERE username = ? AND substr(user_comment, 1, 100) = substr(?, 1, 100)
            '''
            result = self.db_con.execute(query, (username, user_comment)).fetchone()[0]
            return result > 0
        except Exception as e:
            logging.error(f"Error checking for duplicate entry: {e}")
            return False

    def insert_into_db(self, data):
        try:
            self.db_con.execute('''
                INSERT INTO images (
                    id, url, hash, createdAt, postId, username, web_url, tags, user_comment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('id'),
                data.get('url'),
                data.get('hash'),
                data.get('createdAt'),
                data.get('postId'),
                data.get('username'),
                data.get('web_url'),
                data.get('tags'),
                data.get('user_comment')
            ))
            self.db_con.commit()
            self.existing_entries_set.add((tuple(data['tags']), data['user_comment']))
            logging.info("Data inserted into database successfully!")
        except Exception as e:
            logging.error(f"Error inserting data into database: {e}")

    def process_images(self):
        page = 1

        while True:
            cycle_start_time = time.time()
            images = self.fetch_images(page=page)

            if not images:
                logging.info("No new images found on current page. Moving to next page.")
                page += 1
                time.sleep(CONFIG['REFRESH_INTERVAL'])
                continue

            for image in images:
                if image.get('username') in self.recent_users and (datetime.now() - self.recent_users[image.get('username')]).total_seconds() < CONFIG['TIME_THRESHOLD']:
                    self.skipped_count += 1
                    logging.info(f"Skipping image ID: {image['id']} - Recent post from user {image['username']}.")
                    continue
                if image['id'] in self.processed_ids:
                    self.skipped_count += 1
                    logging.info(f"Skipping image ID: {image['id']} - Already processed.")
                    continue

                processed_image = self.process_image(image)
                if processed_image:
                    username = processed_image.get('username')
                    user_comment = processed_image.get('user_comment')

                    if self.is_duplicate(username, user_comment):
                        logging.info(f"Skipping image ID: {processed_image['id']} - Duplicate entry found.")
                        self.skipped_count += 1
                        self.consecutive_duplicate_count += 1
                    elif self.is_similar_entry(processed_image):
                        logging.info(f"Skipping image ID: {processed_image['id']} - Similar entry found.")
                        self.skipped_count += 1
                        self.consecutive_duplicate_count += 1
                    else:
                        self.insert_into_db(processed_image)
                        self.recent_users[username] = datetime.now()
                        self.previous_images.append(processed_image)
                        self.processed_ids.add(processed_image['id'])
                        self.processed_count += 1
                        self.consecutive_duplicate_count = 0
                    if len(self.previous_images) > CONFIG['IMAGES_TO_PROCESS']:
                        self.previous_images.pop(0)

                    logging.info(f"Processed image ID: {processed_image['id']}. Total processed: {self.processed_count}, Total skipped: {self.skipped_count}")

                if self.processed_count >= CONFIG['PROCESS_THRESHOLD']:
                    logging.info("Threshold reached. Restarting process...")
                    time.sleep(CONFIG['PAUSE_DURATION'])
                    self.processed_count = 0
                    self.skipped_count = 0
                    page = 1
                    break

                if self.consecutive_duplicate_count >= 10:
                    logging.info("10 consecutive duplicate entries found. Terminating the script.")
                    return

            cycle_duration = time.time() - cycle_start_time
            time_to_next_cycle = max(0, CONFIG['REFRESH_INTERVAL'] - cycle_duration)
            logging.info(f"Cycle complete. Next cycle in {time_to_next_cycle:.0f} seconds.")
            time.sleep(time_to_next_cycle)

    @staticmethod
    def is_similar(current, previous):
        if not previous:
            return False
        current_prompt = current.get('user_comment', '')
        previous_prompt = previous.get('user_comment', '')
        similarity = SequenceMatcher(None, current_prompt, previous_prompt).ratio()
        return similarity > CONFIG['SIMILARITY_THRESHOLD']

def main():
    logging.info("Image processing started. Press Ctrl+C to stop.")
    try:
        with ImageProcessor() as processor:
            processor.process_images()
    except KeyboardInterrupt:
        logging.info(f"Process stopped. Total processed: {processor.processed_count}, Total skipped: {processor.skipped_count}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    main()